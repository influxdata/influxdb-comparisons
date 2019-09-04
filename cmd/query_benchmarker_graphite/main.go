// query_benchmarker speed tests Graphite using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"github.com/influxdata/influxdb-comparisons/bulk_query/http"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"io"
	"log"
	"sync"
	"time"
)

// Program option vars:
type GraphiteQueryBenchmarker struct {
	daemonUrl     string

	dialTimeout        time.Duration
	readTimeout        time.Duration
	writeTimeout       time.Duration
	httpClientType     string
	scanFinished       bool

	queryPool sync.Pool
	queryChan chan []*http.Query
}

var querier = &GraphiteQueryBenchmarker{}

// Parse args:
func init() {

	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()

}

func (b *GraphiteQueryBenchmarker) Init() {
	flag.StringVar(&b.daemonUrl, "urls", "http://localhost:8080", "Graphite URL.")
	flag.DurationVar(&b.dialTimeout, "dial-timeout", time.Second*15, "TCP dial timeout.")
	flag.DurationVar(&b.readTimeout, "write-timeout", time.Second*300, "TCP write timeout.")
	flag.DurationVar(&b.writeTimeout, "read-timeout", time.Second*300, "TCP read timeout.")
	flag.StringVar(&b.httpClientType, "http-client-type", "fast", "HTTP client type {fast, default}")
}

func (b *GraphiteQueryBenchmarker) Validate() {
	fmt.Printf("Graphite URL: %v\n", b)

	if b.httpClientType == "fast" || b.httpClientType == "default" {
		fmt.Printf("Using HTTP client: %v\n", b.httpClientType)
		http.UseFastHttp = b.httpClientType == "fast"
	} else {
		log.Fatalf("Unsupported HTPP client type: %v", b.httpClientType)
	}
}

func (b *GraphiteQueryBenchmarker) Prepare() {
	// Make pools to minimize heap usage:
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &http.Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				Method:           make([]byte, 0, 1024),
				Path:             make([]byte, 0, 1024),
				Body:             make([]byte, 0, 1024),
			}
		},
	}

	// Make data and control channels:
	b.queryChan = make(chan []*http.Query)
}

func (b *GraphiteQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}
func (b *GraphiteQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *GraphiteQueryBenchmarker) PrepareProcess(i int) {
}

func (b *GraphiteQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	w := http.NewHTTPClient(b.daemonUrl, bulk_query.Benchmarker.Debug(), b.dialTimeout, b.readTimeout, b.writeTimeout)
	b.processQueries(w, workersGroup, statPool, statChan)
}

func (b *GraphiteQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}

func (b *GraphiteQueryBenchmarker) CleanUp() {
	close(b.queryChan)
}

func (b GraphiteQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	params.DBType = "Graphite"
	params.DestinationUrl = b.daemonUrl
	updatedTags = reportTags
	updatedExtraVals = extraVals
	return
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

var qind int64

// scan reads encoded Queries and places them onto the workqueue.
func (b *GraphiteQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(r)

	batch := make([]*http.Query, 0, bulk_query.Benchmarker.BatchSize())

	i := 0
loop:
	for {
		if bulk_query.Benchmarker.Limit() >= 0 && qind >= bulk_query.Benchmarker.Limit() {
			break
		}

		q := b.queryPool.Get().(*http.Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = qind
		batch = append(batch, q)
		i++
		if i == bulk_query.Benchmarker.BatchSize() {
			b.queryChan <- batch
			//batch = batch[:0]
			batch = nil
			batch = make([]*http.Query, 0, bulk_query.Benchmarker.BatchSize())
			i = 0
		}

		qind++
		select {
		case <-closeChan:
			log.Println("Received finish request")
			break loop
		default:
		}

	}
	b.scanFinished = true
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func (b *GraphiteQueryBenchmarker) processQueries(w http.HTTPClient, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) error {
	opts := &http.HTTPClientDoOptions{
		Debug:                bulk_query.Benchmarker.Debug(),
		PrettyPrintResponses: bulk_query.Benchmarker.PrettyPrintResponses(),
	}
	var queriesSeen int64
	for queries := range b.queryChan {
		if len(queries) == 1 {
			if err := b.processSingleQuery(w, queries[0], opts, nil, nil, statPool, statChan); err != nil {
				log.Fatal(err)
			}
			queriesSeen++
		} else {
			var err error
			errors := 0
			done := 0
			errCh := make(chan error)
			doneCh := make(chan int, len(queries))
			for _, q := range queries {
				go b.processSingleQuery(w, q, opts, errCh, doneCh, statPool, statChan)
				queriesSeen++
			}

		loop:
			for {
				select {
				case err = <-errCh:
					errors++
				case <-doneCh:
					done++
					if done == len(queries) {
						break loop
					}
				}
			}
			close(errCh)
			close(doneCh)
			if err != nil {
				log.Fatal(err)
			}
		}
		if bulk_query.Benchmarker.WaitInterval().Seconds() > 0 {
			time.Sleep(bulk_query.Benchmarker.WaitInterval())
		}
	}
	workersGroup.Done()
	return nil
}

func (b *GraphiteQueryBenchmarker) processSingleQuery(w http.HTTPClient, q *http.Query, opts *http.HTTPClientDoOptions, errCh chan error, doneCh chan int, statPool sync.Pool, statChan chan *bulk_query.Stat) error {
	defer func() {
		if doneCh != nil {
			doneCh <- 1
		}
	}()
	lagMillis, err := w.Do(q, opts)
	stat := statPool.Get().(*bulk_query.Stat)
	stat.Init(q.HumanLabel, lagMillis)
	statChan <- stat
	b.queryPool.Put(q)
	if err != nil {
		qerr := fmt.Errorf("Error during request of query %s: %s\n", q.String(), err.Error())
		if errCh != nil {
			errCh <- qerr
			return nil
		} else {
			return qerr
		}
	}

	return nil
}
