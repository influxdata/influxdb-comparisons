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
	"io"
	"log"
	"os"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"strconv"
	"strings"
)

// Program option vars:
var (
	daemonUrl      string
	workers        int
	batchSize      int
	limit          int64
	doLoad         bool
	writeTimeout   time.Duration
	reportDatabase string
	reportHost     string
	reportUser     string
	reportPassword string
	reportTagsCSV  string
)

// Global vars
var (
	batchChan      chan *Batch
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	reportTags     [][2]string
	reportHostname string
)

// Magic database constants
const (
	dbName              = "benchmark_db"
	pointCollectionName = "point_data"
)

// bufPool holds []byte instances to reduce heap churn.
var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Batch holds byte slices that will become mongo_serialization.Item instances.
type Batch [][]byte

func (b *Batch) ClearReferences() {
	*b = (*b)[:0]
}

// batchPool holds *Batch instances to reduce heap churn.
var batchPool = &sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:27017", "Mongo URL.")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&limit, "limit", -1, "Number of items to insert (default unlimited).")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")

	flag.Parse()

	for i := 0; i < workers*batchSize; i++ {
		bufPool.Put(bufPool.New())
	}

	if reportHost != "" {
		fmt.Printf("results report destination: %v\n", reportHost)
		fmt.Printf("results report database: %v\n", reportDatabase)

		var err error
		reportHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("hostname for results report: %v\n", reportHostname)

		if reportTagsCSV != "" {
			pairs := strings.Split(reportTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				reportTags = append(reportTags, tagpair)
			}
		}
		fmt.Printf("results report tags: %v\n", reportTags)
	}
}

func main() {
	if doLoad {
		mustCreateCollections(daemonUrl)
	}

	var session *mgo.Session

	if doLoad {
		var err error
		session, err = mgo.Dial(daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		session.SetMode(mgo.Eventual, false)
		session.SetSafe(&mgo.Safe{J: true})

		defer session.Close()
	}

	batchChan = make(chan *Batch, workers*10)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(session)
	}

	start := time.Now()
	itemsRead, bytesRead := scan(session, batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	itemRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())

	fmt.Printf("loaded %d values in %fsec with %d workers (mean values rate %f values/sec, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		reportTags = append(reportTags, [2]string{"write_timeout", strconv.Itoa(int(writeTimeout))})

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "MongoDB",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     daemonUrl,
				Workers:            workers,
				ItemLimit:          int(limit),
			},
			IsGzip:    false,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, itemRate, bytesRate, took)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads length-delimited flatbuffers items from stdin.
func scan(session *mgo.Session, itemsPerBatch int) (int64, int64) {
	var n int
	var itemsRead, bytesRead int64
	r := bufio.NewReaderSize(os.Stdin, 32<<20)

	start := time.Now()
	batch := batchPool.Get().(*Batch)
	lenBuf := make([]byte, 8)

	for {
		if itemsRead == limit {
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
		l := int(binary.LittleEndian.Uint64(lenBuf))
		itemBuf := bufPool.Get().([]byte)
		if cap(itemBuf) < l {
			itemBuf = make([]byte, l)
		}
		itemBuf = itemBuf[:l]

		// read the bytes and init the flatbuffer object
		totRead := 0
		for totRead < l {
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

		itemsRead++
		n++

		if n >= batchSize {
			bytesRead += int64(len(itemBuf))
			batchChan <- batch
			n = 0
			batch = batchPool.Get().(*Batch)
		}

		_ = start
		//if itemsRead > 0 && itemsRead%100000 == 0 {
		//	_ = start
		//	//took := (time.Now().UnixNano() - start.UnixNano())
		//	//if took >= 1e9 {
		//	//	tookUs := float64(took) / 1e3
		//	//	tookSec := float64(took) / 1e9
		//	//	fmt.Fprintf(os.Stderr, "itemsRead: %d, rate: %.0f/sec, lag: %.2fus/op\n",
		//	//		itemsRead, float64(itemsRead)/tookSec, tookUs/float64(itemsRead))
		//	//}
		//}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead, bytesRead
}

// processBatches reads byte buffers from batchChan, interprets them and writes
// them to the target server. Note that mgo forcibly incurs serialization
// overhead (it always encodes to BSON).
func processBatches(session *mgo.Session) {
	db := session.DB(dbName)

	type Tag struct {
		Key string `bson:"key"`
		Val string `bson:"val"`
	}

	type Point struct {
		// Use `string` here even though they are really `[]byte`.
		// This is so the mongo data is human-readable.
		MeasurementName string      `bson:"measurement"`
		FieldName       string      `bson:"field"`
		Timestamp       int64       `bson:"timestamp_ns"`
		Tags            []Tag       `bson:"tags"`
		Value           interface{} `bson:"value"`

		// a private union-like section
		longValue   int64
		doubleValue float64
		stringValue string
	}
	pPool := &sync.Pool{New: func() interface{} { return &Point{} }}
	pvs := []interface{}{}

	item := &mongo_serialization.Item{}
	destTag := &mongo_serialization.Tag{}
	collection := db.C(pointCollectionName)
	for batch := range batchChan {
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
			x.FieldName = unsafeBytesToString(item.FieldNameBytes())
			x.Timestamp = item.TimestampNanos()

			tagLength := item.TagsLength()
			if cap(x.Tags) < tagLength {
				x.Tags = make([]Tag, 0, tagLength)
			}
			x.Tags = x.Tags[:tagLength]
			for i := 0; i < tagLength; i++ {
				*destTag = mongo_serialization.Tag{} // clear
				item.Tags(destTag, i)
				x.Tags[i].Key = unsafeBytesToString(destTag.KeyBytes())
				x.Tags[i].Val = unsafeBytesToString(destTag.ValBytes())
			}

			// this complexity is the result of trying to minimize
			// allocs while using an interface{} type for
			// (*Point).Value.
			switch item.ValueType() {
			case mongo_serialization.ValueTypeLong:
				x.longValue = item.LongValue()
				x.Value = &x.longValue
			case mongo_serialization.ValueTypeDouble:
				x.doubleValue = item.DoubleValue()
				x.Value = &x.doubleValue
			case mongo_serialization.ValueTypeString:
				x.stringValue = unsafeBytesToString(item.StringValueBytes())
				x.Value = &x.stringValue
			default:
				panic("logic error")
			}
			pvs[i] = x

		}
		bulk.Insert(pvs...)

		if doLoad {
			_, err := bulk.Run()
			if err != nil {
				log.Fatalf("Bulk err: %s\n", err.Error())
			}

		}

		// cleanup pvs
		for _, x := range pvs {
			p := x.(*Point)
			p.Timestamp = 0
			p.Value = nil
			p.longValue = 0
			p.doubleValue = 0
			p.Tags = p.Tags[:0]
			pPool.Put(p)
		}

		// cleanup item data
		for _, itemBuf := range *batch {
			bufPool.Put(itemBuf)
		}
		batch.ClearReferences()
		batchPool.Put(batch)
	}
	workersGroup.Done()
}

func mustCreateCollections(daemonUrl string) {
	session, err := mgo.Dial(daemonUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// collection C: point data
	// from (*mgo.Collection).Create
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"create", pointCollectionName})

	// wiredtiger settings
	cmd = append(cmd, bson.DocElem{
		"storageEngine", map[string]interface{}{
			"wiredTiger": map[string]interface{}{
				"configString": "block_compressor=snappy",
			},
		},
	})

	err = session.DB("benchmark_db").Run(cmd, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := session.DB("benchmark_db").C("point_data")
	index := mgo.Index{
		Key:        []string{"measurement", "tags", "field", "timestamp_ns"},
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
