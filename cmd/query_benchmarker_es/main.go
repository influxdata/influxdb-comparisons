// query_benchmarker speed tests ElasticSearch using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program currently has no knowledge of
// the internals of the endpoint.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/influxdata/influxdb-comparisons/util/report"
)

type ElasticQueryBenchmarker struct {
	// Program option vars:
	csvDaemonUrls string
	daemonUrls    []string
	// Global vars:
	queryPool    sync.Pool
	queryChan    chan *Query
	scanFinished bool
}

var querier = &ElasticQueryBenchmarker{}

// Parse args:
func init() {
	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

func (b *ElasticQueryBenchmarker) Init() {
	flag.StringVar(&b.csvDaemonUrls, "urls", "http://localhost:9200", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
}

func (b *ElasticQueryBenchmarker) Validate() {
	b.daemonUrls = strings.Split(b.csvDaemonUrls, ",")
	if len(b.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", b.daemonUrls)
}

func (b *ElasticQueryBenchmarker) Prepare() {
	// Make pools to minimize heap usage:
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				Method:           make([]byte, 0, 1024),
				Path:             make([]byte, 0, 1024),
				Body:             make([]byte, 0, 1024),
			}
		},
	}
	b.queryChan = make(chan *Query)
}

func (b *ElasticQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}

func (b *ElasticQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *ElasticQueryBenchmarker) PrepareProcess(i int) {

}

func (b *ElasticQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	daemonUrl := b.daemonUrls[i%len(b.daemonUrls)]
	w := NewHTTPClient(daemonUrl, bulk_query.Benchmarker.Debug())
	b.processQueries(w, workersGroup, statPool, statChan)
}

func (b *ElasticQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}

func (b *ElasticQueryBenchmarker) CleanUp() {
	close(b.queryChan)
}

func (b *ElasticQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	params.DBType = "ElasticSearch"
	params.DestinationUrl = b.csvDaemonUrls
	updatedTags = reportTags
	updatedExtraVals = extraVals
	return
}

// scan reads encoded Queries and places them onto the workqueue.
func (b *ElasticQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
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
			log.Fatal(err)
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

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func (b *ElasticQueryBenchmarker) processQueries(w *HTTPClient, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	opts := &HTTPClientDoOptions{
		Debug:                bulk_query.Benchmarker.Debug(),
		PrettyPrintResponses: bulk_query.Benchmarker.PrettyPrintResponses(),
	}
	var queriesSeen int64
	for q := range b.queryChan {
		lagMillis, err := w.Do(q, opts)

		stat := statPool.Get().(*bulk_query.Stat)
		stat.Init(q.HumanLabel, lagMillis)
		statChan <- stat

		b.queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}

		queriesSeen++
	}
	workersGroup.Done()
}
