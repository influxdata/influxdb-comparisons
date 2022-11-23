// query_benchmarker_mongo speed tests Mongo using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided Mongo endpoint.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/util/report"
)

type MongoQueryBenchmarker struct {
	// Program option vars:
	daemonUrl string
	doQueries bool
	// Global vars:
	queryPool    sync.Pool
	queryChan    chan *Query
	scanFinished bool
	client       *mongo.Client
}

var querier = &MongoQueryBenchmarker{}

// Parse args:
func init() {
	// needed for deserializing the mongo query from gob
	gob.Register(mongodb.S{})
	gob.Register(mongodb.M{})
	gob.Register([]mongodb.M{})
	gob.Register(time.Time{})

	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()
}

func (b *MongoQueryBenchmarker) Init() {
	flag.StringVar(&b.daemonUrl, "url", "mongodb://localhost:27017", "MongoDB URL.")
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
	b.client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(b.daemonUrl))
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
	type Handle struct {
		collection *mongo.Collection
		typ        string
	}
	handles := make(map[string]*Handle, 0)

	c := func(q *Query) *Handle {
		cn := unsafeBytesToString(q.CollectionName)
		h, ok := handles[cn]
		if !ok {
			dn := unsafeBytesToString(q.DatabaseName)
			log.Printf("get collection handle for %s/%s\n", dn, cn)
			db := b.client.Database(dn)
			collection := db.Collection(cn)
			specs, err := db.ListCollectionSpecifications(context.TODO(), bson.M{ "name": cn })
			if err != nil {
				log.Fatalf("db ListCollectionSpecifications error: %v", err)
			}
			if specs == nil || len(specs) == 0 {
				log.Fatalf("collection '%s' not found", cn)
			}
			log.Printf("collection type: %s\n", specs[0].Type)
			h = &Handle{
				collection: collection,
				typ: specs[0].Type,
			}
			handles[cn] = h
		}
		return h
	}

	for q := range b.queryChan {
		h := c(q)
		lag, err := b.oneQuery(h.collection, q, h.typ, context.TODO())
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
func (b *MongoQueryBenchmarker) oneQuery(collection *mongo.Collection, q *Query, typ string, ctx context.Context) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	if b.doQueries {
		pipe := q.BsonDoc
		cursor, err := collection.Aggregate(ctx, pipe)
		if err != nil {
			return 0, err
		}

		var result result
		if typ == "timeseries" {
			result = &resultT{}
		} else {
			result = &resultC{}
		}

		for cursor.Next(ctx) {
			err = cursor.Decode(result)
			if err != nil {
				return 0, err
			}
			if bulk_query.Benchmarker.PrettyPrintResponses() {
				fmt.Printf("ID %d: %s, %f\n", q.ID, result.time(), result.value())
			}
		}

		err = cursor.Close(ctx)
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}

type result interface {
	time() time.Time
	value() float64
}

type resultC struct {
	Id struct {
		TimeBucket int64 `bson:"time_bucket"`
	} `bson:"_id"`
	Value float64 `bson:"agg_value"`
}

func (r resultC) time() time.Time {
	return time.Unix(0, r.Id.TimeBucket).UTC()
}

func (r resultC) value() float64 {
	return r.Value
}

type resultT struct {
	Id struct {
		TimeBucket time.Time `bson:"time_bucket"`
	} `bson:"_id"`
	Value float64 `bson:"agg_value"`
}

func (r resultT) time() time.Time {
	return r.Id.TimeBucket
}

func (r resultT) value() float64 {
	return r.Value
}
