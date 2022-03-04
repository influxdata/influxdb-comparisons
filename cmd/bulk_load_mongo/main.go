// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
//
// When Mongo 5.0 time series collection is used, measurement name must be
// serialized in tags, because index on time series collection can only be applied
// on time and meta field. Using time series implies flat format.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/google/flatbuffers/go"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
	"github.com/influxdata/influxdb-comparisons/util/report"
)

// Magic database constants
const (
	pointCollectionName = "point_data"
)

// Batch holds byte slices that will become mongo_serialization.Item instances.
type Batch [][]byte

func (b *Batch) ClearReferences() {
	*b = (*b)[:0]
}

type MongoBulkLoad struct {
	// Program option vars:
	daemonUrl      string
	documentFormat string
	writeTimeout   time.Duration
	// Global vars
	batchChan    chan *Batch
	inputDone    chan struct{}
	valuesRead   int64
	itemsRead    int64
	bytesRead    int64
	bufPool      *sync.Pool
	batchPool    *sync.Pool
	bsonDPool    *sync.Pool
	scanFinished bool
}

var load = &MongoBulkLoad{}

// Parse args:
func init() {
	bulk_load.Runner.Init(100)
	load.Init()

	flag.Parse()

	bulk_load.Runner.Validate()
	load.Validate()

}

func main() {
	bulk_load.Runner.Run(load)
}

func (l *MongoBulkLoad) Init() {
	flag.StringVar(&l.daemonUrl, "url", "mongodb://localhost:27017", "MongoDB URL.")
	flag.DurationVar(&l.writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")
	flag.StringVar(&l.documentFormat, "document-format", "timeseries", "Document format flags ('key-pair', 'flat', 'timeseries' - default)")
}

func (l *MongoBulkLoad) Validate() {
	if strings.Contains(l.documentFormat, mongodb.FlatFormat) {
		mongodb.DocumentFormat = mongodb.FlatFormat
	} else {
		mongodb.DocumentFormat = mongodb.KeyPairFormat
	}
	mongodb.UseTimeseries = strings.Contains(l.documentFormat, mongodb.TimeseriesFormat)
	if mongodb.UseTimeseries {
		log.Print("Using MongoDB 5+ time series collection")
		mongodb.DocumentFormat = mongodb.FlatFormat
	}
	log.Printf("Using %s point serialization", mongodb.DocumentFormat)
}

func (l *MongoBulkLoad) CreateDb() {
	mustCreateCollections(l.daemonUrl, bulk_load.Runner.DbName)
}

func (l *MongoBulkLoad) PrepareWorkers() {

	// bufPool holds []byte instances to reduce heap churn.
	l.bufPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}

	// batchPool holds *Batch instances to reduce heap churn.
	l.batchPool = &sync.Pool{
		New: func() interface{} {
			return &Batch{}
		},
	}

	// bsonDPool holds bsonD instances to reduce heap churn.
	l.bsonDPool = &sync.Pool{
		New: func() interface{} {
			return make(bson.D, 0)
		},
	}

	for i := 0; i < bulk_load.Runner.Workers*bulk_load.Runner.BatchSize; i++ {
		l.bufPool.Put(l.bufPool.New())
	}

	l.batchChan = make(chan *Batch, bulk_load.Runner.Workers*10)
	l.inputDone = make(chan struct{})
}

func (l *MongoBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *MongoBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *MongoBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *MongoBulkLoad) CleanUp() {
}

func (l *MongoBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {
	reportTags = [][2]string{{"write_timeout", strconv.Itoa(int(l.writeTimeout))}}
	params.DBType = "MongoDB"
	params.DestinationUrl = l.daemonUrl
	return
}

func (l *MongoBulkLoad) PrepareProcess(i int) {
}

func (l *MongoBulkLoad) AfterRunProcess(i int) {

}

func (l *MongoBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *MongoBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

func (l *MongoBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

// scan reads length-delimited flatbuffers items from stdin.
func (l *MongoBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0

	var n int
	br := bufio.NewReaderSize(r, 32<<20)
	start := time.Now()
	batch := l.batchPool.Get().(*Batch)
	lenBuf := make([]byte, 8)
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for {
		if l.itemsRead == bulk_load.Runner.ItemLimit {
			break
		}
		// get the serialized item length (this is the framing format)
		_, err := io.ReadFull(br, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}

		// ensure correct len of receiving buffer
		d := int(binary.LittleEndian.Uint64(lenBuf))
		itemBuf := l.bufPool.Get().([]byte)
		if cap(itemBuf) < d {
			itemBuf = make([]byte, d)
		}
		itemBuf = itemBuf[:d]

		// get the item
		_, err = io.ReadFull(br, itemBuf)
		if err != nil {
			log.Fatal(err.Error())
		}

		*batch = append(*batch, itemBuf)

		l.itemsRead++
		n++

		if n >= bulk_load.Runner.BatchSize {
			l.bytesRead += int64(len(itemBuf))
			l.batchChan <- batch
			n = 0
			batch = l.batchPool.Get().(*Batch)
			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}

		}

		_ = start
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	// send outstanding batch
	if n > 0 {
		l.batchChan <- batch
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)
	l.scanFinished = true
}

// processBatches reads byte buffers from batchChan, interprets them and writes
// them to the target server. Note that mgo forcibly incurs serialization
// overhead (it always encodes to BSON).
func (l *MongoBulkLoad) RunProcess(i int, workersGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	var workerValuesRead int64
	var rerr error
	var pvs []interface{}

	item := &mongo_serialization.Item{}
	destTag := &mongo_serialization.Tag{}
	destField := &mongo_serialization.Field{}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(l.daemonUrl))
	if err != nil {
		return err
	}
	db := client.Database(bulk_load.Runner.DbName)
	collection := db.Collection(pointCollectionName)
outer:
	for batch := range l.batchChan {

		if cap(pvs) < len(*batch) {
			pvs = make([]interface{}, len(*batch))
		}
		pvs = pvs[:len(*batch)]

		for i, itemBuf := range *batch {
			n := flatbuffers.GetUOffsetT(itemBuf)
			item.Init(itemBuf, n)

			doc := l.bsonDPool.Get().(bson.D)
			if mongodb.UseTimeseries {
				// timeseries implies measurement is in meta field ie. tags
				doc = append(doc, bson.E{ Key: "timestamp", Value: time.Unix(0, item.TimestampNanos())})
			} else {
				doc = append(doc, bson.E{ Key: "measurement", Value: unsafeBytesToString(item.MeasurementNameBytes()) })
				doc = append(doc, bson.E{ Key:"timestamp_ns", Value: item.TimestampNanos()})
			}

			var tags interface{}
			var tagsM bson.M
			var tagsA bson.A
			tagLength := item.TagsLength()
			if mongodb.DocumentFormat == mongodb.FlatFormat {
				tagsM = make(bson.M, tagLength)
				tags = tagsM
			} else {
				tagsA = make(bson.A, tagLength)
				tags = tagsA
			}
			if mongodb.UseTimeseries {
				tagsM["measurement"] = unsafeBytesToString(item.MeasurementNameBytes())
			}
			for i := 0; i < tagLength; i++ {
				*destTag = mongo_serialization.Tag{} // clear
				item.Tags(destTag, i)
				tagKey := unsafeBytesToString(destTag.KeyBytes())
				tagValue := unsafeBytesToString(destTag.ValBytes())
				if mongodb.DocumentFormat == mongodb.FlatFormat {
					tagsM[tagKey] = tagValue
				} else {
					tagsA[i] = bson.D{{"key", tagKey }, {"val", tagValue }}
				}
			}
			doc = append(doc, bson.E{ Key: "tags", Value: tags })

			var fields interface{}
			var fieldsM bson.M
			var fieldsA bson.A
			fieldLength := item.FieldsLength()
			if mongodb.DocumentFormat == mongodb.FlatFormat {
				fieldsM = make(bson.M, fieldLength)
				fields = fieldsM
			} else {
				fieldsA = make(bson.A, fieldLength)
				fields = fieldsA
			}
			for i := 0; i < fieldLength; i++ {
				*destField = mongo_serialization.Field{} // clear
				item.Fields(destField, i)
				fieldKey := unsafeBytesToString(destField.KeyBytes())
				var fieldValue interface{}
				switch destField.ValueType() {
				case mongo_serialization.ValueTypeInt:
					fieldValue = destField.IntValue()
				case mongo_serialization.ValueTypeLong:
					fieldValue = destField.LongValue()
				case mongo_serialization.ValueTypeFloat:
					fieldValue = destField.FloatValue()
				case mongo_serialization.ValueTypeDouble:
					fieldValue = destField.DoubleValue()
				case mongo_serialization.ValueTypeString:
					fieldValue = unsafeBytesToString(destField.StringValueBytes())
				default:
					panic("logic error")
				}
				if mongodb.DocumentFormat == mongodb.FlatFormat {
					fieldsM[fieldKey] = fieldValue
				} else {
					fieldsA[i] = bson.D{{"key", fieldKey }, { "val", fieldValue }}
				}
			}
			doc = append(doc, bson.E{ Key: "fields", Value: fields })

			pvs[i] = doc
			workerValuesRead += int64(fieldLength)
		}

		if bulk_load.Runner.DoLoad {
			f := false
			opts := &options.InsertManyOptions{
				Ordered: &f,
			}
			_, err := collection.InsertMany(context.TODO(), pvs, opts)
			if err != nil {
				rerr = fmt.Errorf("collection InsertMany err: %s\n", err.Error())
				break outer
			}
		}

		// reuse bson data
		for _, doc := range pvs {
			d := doc.(bson.D)
			d = d[:0]
			l.bsonDPool.Put(d)
		}

		// cleanup item data
		for _, itemBuf := range *batch {
			l.bufPool.Put(itemBuf)
		}
		batch.ClearReferences()
		l.batchPool.Put(batch)
	}

	atomic.AddInt64(&l.valuesRead, workerValuesRead)
	workersGroup.Done()
	return rerr
}

func mustCreateCollections(daemonUrl string, dbName string) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(daemonUrl))
	if err != nil {
		log.Fatal(err)
	}

	meta := "tags"
	var opts *options.CreateCollectionOptions
	if mongodb.UseTimeseries {
		opts = &options.CreateCollectionOptions{
			TimeSeriesOptions: &options.TimeSeriesOptions{
				TimeField: "timestamp",
				MetaField: &meta,
			},
		}
	}

	db := client.Database(dbName)
	err = db.CreateCollection(context.TODO(), pointCollectionName, opts)
	if err != nil {
		log.Fatalf("CreateCollection: %v", err)
	}

	var indexKeys bson.D
	if mongodb.UseTimeseries {
		// in the future, use text or hashed index type for tags, but with 5.0, it is not possible:
		// - text index not supported on time-series collections
		// - hashed indexes do not currently support array values
		indexKeys = bson.D{{"tags", 1}, {"timestamp", 1}}
	} else {
		indexKeys = bson.D{{"measurement", "text"}, {"tags", "text"}, {"timestamp_ns", 1}}
	}

	f := false
	collection := db.Collection(pointCollectionName)
	index := mongo.IndexModel{
		Keys: indexKeys,
		Options: &options.IndexOptions{
			Unique: &f, // Unique does not work on the entire array of tags!
			//	DropDups:   true, // mgo driver option, missing in mongo driver
			Background: &f,
			Sparse: &f,
		},
	}

	_, err = collection.Indexes().CreateOne(context.TODO(), index)
	if err != nil {
		log.Fatalf("index CreateOne: %v", err)
	}
}
