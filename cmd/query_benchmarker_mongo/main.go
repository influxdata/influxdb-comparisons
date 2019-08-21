// query_benchmarker_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint using mgo.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"gopkg.in/mgo.v2"
	"io"
	"log"
	"sync"
	"time"
)

type MongoQueryBenchmarker struct {
	// Program option vars:
	daemonUrl string
	doQueries bool
	// Global vars:
	queryPool    sync.Pool
	queryChan    chan *Query
	scanFinished bool
	session      *mgo.Session
}

var querier = &MongoQueryBenchmarker{}

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register(mongodb.S{})
	gob.Register(mongodb.M{})
	gob.Register([]mongodb.M{})

	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()
}

func (b *MongoQueryBenchmarker) Init() {
	flag.StringVar(&b.daemonUrl, "url", "mongodb://localhost:27017", "Daemon URL.")
	flag.BoolVar(&b.doQueries, "do-queries", true, "Whether to perform queries (useful for benchmarking the query executor.)")

}

func (b *MongoQueryBenchmarker) Validate() {

}

func (b *MongoQueryBenchmarker) Prepare() {
	var err error
	// Make pools to minimize heap usage:
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				BsonDoc:          nil,
			}
		},
	}
	b.queryChan = make(chan *Query)
	b.session, err = mgo.Dial(b.daemonUrl)
	if err != nil {
		log.Fatal(err)
	}

}

func (b *MongoQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}

func (b *MongoQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *MongoQueryBenchmarker) CleanUp() {
	close(b.queryChan)
}

func (b *MongoQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}

func (b *MongoQueryBenchmarker) PrepareProcess(i int) {

}

func (b *MongoQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	params.DBType = "MongoDB"
	params.DestinationUrl = b.daemonUrl
	updatedTags = reportTags
	updatedExtraVals = extraVals
	return
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func (b *MongoQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	for q := range b.queryChan {
		lag, err := b.oneQuery(b.session, q)

		stat := statPool.Get().(*bulk_query.Stat)
		stat.Init(q.HumanLabel, lag)
		statChan <- stat

		b.queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

// scan reads encoded Queries and places them onto the workqueue.
func (b *MongoQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(r)

	n := int64(0)
loop:
	for {
		if bulk_query.Benchmarker.Limit() >= 0 && n >= bulk_query.Benchmarker.Limit() {
			break
		}

		q := b.queryPool.Get().(*Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("decoder", err)
		}

		q.ID = n

		b.queryChan <- q

		n++

		select {
		case <-closeChan:
			fmt.Printf("Received finish request\n")
			break loop
		default:
		}

	}
	b.scanFinished = true
}

// oneQuery executes on Query
func (b *MongoQueryBenchmarker) oneQuery(session *mgo.Session, q *Query) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	if b.doQueries {
		db := session.DB(unsafeBytesToString(q.DatabaseName))
		//fmt.Printf("db: %#v\n", db)
		collection := db.C(unsafeBytesToString(q.CollectionName))
		//fmt.Printf("collection: %#v\n", collection)
		pipe := collection.Pipe(q.BsonDoc)
		iter := pipe.Iter()
		type Result struct {
			Id struct {
				TimeBucket int64 `bson:"time_bucket"`
			} `bson:"_id"`
			Value float64 `bson:"agg_value"`
		}

		result := Result{}
		for iter.Next(&result) {
			if bulk_query.Benchmarker.PrettyPrintResponses() {
				t := time.Unix(0, result.Id.TimeBucket).UTC()
				fmt.Printf("ID %d: %s, %f\n", q.ID, t, result.Value)
			}
		}

		err = iter.Close()
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}
