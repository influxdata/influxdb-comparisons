// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/flatbuffers/go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"strconv"
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
	session      *mgo.Session
	scanFinished bool
}

func (l *MongoBulkLoad) Init() {
	flag.StringVar(&l.daemonUrl, "url", "localhost:27017", "Mongo URL.")
	flag.DurationVar(&l.writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")
	flag.StringVar(&l.documentFormat, "document-format", "", "Document format specification. ('simpleArrays' is supported; leave empty for previous behaviour)")
}

func (l *MongoBulkLoad) Validate() {
	if l.documentFormat == mongodb.SimpleArraysFormat {
		log.Printf("Using '%s' document serialization", l.documentFormat)
	}
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

	for i := 0; i < bulk_load.Runner.Workers*bulk_load.Runner.BatchSize; i++ {
		l.bufPool.Put(l.bufPool.New())
	}

	if bulk_load.Runner.DoLoad {
		var err error
		l.session, err = mgo.Dial(l.daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		l.session.SetMode(mgo.Eventual, false)
		l.session.SetSyncTimeout(180 * time.Second)
		l.session.SetSocketTimeout(180 * time.Second)

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
	if l.session != nil {
		l.session.Close()
	}
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

// scan reads length-delimited flatbuffers items from stdin.
func (l *MongoBulkLoad) RunScanner(syncChanDone chan int) {
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	var n int
	r := bufio.NewReaderSize(os.Stdin, 32<<20)

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
		_, err := r.Read(lenBuf)
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

		// read the bytes and init the flatbuffer object
		totRead := 0
		for totRead < d {
			m, err := r.Read(itemBuf[totRead:])
			// (EOF is also fatal)
			if err != nil {
				log.Fatal(err.Error())
			}
			totRead += m
		}
		if totRead != len(itemBuf) {
			panic(fmt.Sprintf("reader/writer logic error, %d != %d", n, len(itemBuf)))
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

	db := l.session.DB(bulk_load.Runner.DbName)

	type Tag struct {
		Key string `bson:"key"`
		Val string `bson:"val"`
	}

	type Field struct {
		Key string      `bson:"key"`
		Val interface{} `bson:"val"`
	}

	type Point struct {
		// Use `string` here even though they are really `[]byte`.
		// This is so the mongo data is human-readable.
		MeasurementName string        `bson:"measurement"`
		Timestamp       int64         `bson:"timestamp_ns"`
		Tags            []interface{} `bson:"tags"`
		Fields          []interface{} `bson:"fields"`
	}
	pPool := &sync.Pool{New: func() interface{} { return &Point{} }}
	pvs := []interface{}{}

	item := &mongo_serialization.Item{}
	destTag := &mongo_serialization.Tag{}
	destField := &mongo_serialization.Field{}
	collection := db.C(pointCollectionName)
	for batch := range l.batchChan {
		bulk := collection.Bulk()

		if cap(pvs) < len(*batch) {
			pvs = make([]interface{}, len(*batch))
		}
		pvs = pvs[:len(*batch)]

		for i, itemBuf := range *batch {
			// this ui could be improved on the library side:
			n := flatbuffers.GetUOffsetT(itemBuf)
			item.Init(itemBuf, n)

			x := pPool.Get().(*Point)

			x.MeasurementName = unsafeBytesToString(item.MeasurementNameBytes())
			x.Timestamp = item.TimestampNanos()

			tagLength := item.TagsLength()
			if cap(x.Tags) < tagLength {
				x.Tags = make([]interface{}, 0, tagLength)
			}
			x.Tags = x.Tags[:tagLength]
			for i := 0; i < tagLength; i++ {
				*destTag = mongo_serialization.Tag{} // clear
				item.Tags(destTag, i)
				tagKey := unsafeBytesToString(destTag.KeyBytes())
				tagValue := unsafeBytesToString(destTag.ValBytes())
				if l.documentFormat == mongodb.SimpleArraysFormat {
					x.Tags[i] = bson.M{tagKey: tagValue}
				} else {
					x.Tags[i] = &Tag{Key: tagKey, Val: tagValue}
				}
			}

			fieldLength := item.FieldsLength()
			if cap(x.Fields) < fieldLength {
				x.Fields = make([]interface{}, 0, fieldLength)
			}
			x.Fields = x.Fields[:fieldLength]
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
				if l.documentFormat == mongodb.SimpleArraysFormat {
					x.Fields[i] = bson.M{fieldKey: fieldValue}
				} else {
					x.Fields[i] = &Field{Key: fieldKey, Val: fieldValue}
				}
			}
			pvs[i] = x
			workerValuesRead += int64(fieldLength)
		}
		bulk.Insert(pvs...)

		if bulk_load.Runner.DoLoad {
			_, err := bulk.Run()
			if err != nil {
				rerr = fmt.Errorf("Bulk err: %s\n", err.Error())
				break
			}

		}

		// cleanup pvs
		for _, x := range pvs {
			p := x.(*Point)
			p.Timestamp = 0
			p.Tags = p.Tags[:0]
			p.Fields = p.Fields[:0]
			pPool.Put(p)
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
	session, err := mgo.Dial(daemonUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// collection C: point data
	// from (*mgo.Collection).Create
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"create", pointCollectionName})

	err = session.DB(dbName).Run(cmd, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := session.DB(dbName).C("point_data")
	index := mgo.Index{
		Key:        []string{"measurement", "tags", "timestamp_ns"},
		Unique:     false, // Unique does not work on the entire array of tags!
		DropDups:   true,
		Background: false,
		Sparse:     false,
	}
	err = collection.EnsureIndex(index)
	if err != nil {
		log.Fatal(err)
	}
}
