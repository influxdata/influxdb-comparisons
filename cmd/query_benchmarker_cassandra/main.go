// query_benchmarker_cassandra speed tests Cassandra servers using request
// data from stdin.
//
// It reads encoded HLQuery objects from stdin, and makes concurrent requests
// to the provided Cassandra cluster. This program is a 'heavy client', i.e.
// it builds a client-side index of table metadata before beginning the
// benchmarking.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"io"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	BlessedKeyspace string = "measurements"
)

type CassandraQueryBenchmarker struct {
	daemonUrl           string
	aggrPlanLabel       string
	subQueryParallelism int
	requestTimeout      time.Duration
	csiTimeout          time.Duration
	//util vars
	queryPool       sync.Pool
	hlQueryChan     chan *HLQuery
	statPool        sync.Pool
	statChan        chan *bulk_query.Stat
	workersGroup    sync.WaitGroup
	statGroup       sync.WaitGroup
	aggrPlan        int
	reportTags      [][2]string
	reportHostname  string
	reportQueryStat bulk_query.StatGroup
	session         *gocql.Session
	queryExecutor   *HLQueryExecutor
	scanFinished    bool
}

// Helpers for choice-like flags:
var (
	aggrPlanChoices = map[string]int{
		"server": AggrPlanTypeWithServerAggregation,
		"client": AggrPlanTypeWithoutServerAggregation,
	}
)

var querier = &CassandraQueryBenchmarker{}

// Parse args:
func init() {
	bulk_query.Benchmarker.Init()
	querier.init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.validate()
}

func (b *CassandraQueryBenchmarker) init() {
	flag.StringVar(&b.daemonUrl, "url", "localhost:9042", "Cassandra URL.")
	flag.StringVar(&b.aggrPlanLabel, "aggregation-plan", "", "Aggregation plan (choices: server, client)")
	flag.IntVar(&b.subQueryParallelism, "subquery-workers", 1, "Number of concurrent subqueries to make (because the client does a scatter+gather operation).")
	flag.DurationVar(&b.requestTimeout, "request-timeout", 60*time.Second, "Maximum request timeout.")
	flag.DurationVar(&b.csiTimeout, "client-side-index-timeout", 10*time.Second, "Maximum client-side index timeout (only used at initialization).")
}

func (b *CassandraQueryBenchmarker) validate() {
	if _, ok := aggrPlanChoices[b.aggrPlanLabel]; !ok {
		log.Fatal("invalid aggregation plan")
	}
	b.aggrPlan = aggrPlanChoices[b.aggrPlanLabel]
}

func (b *CassandraQueryBenchmarker) Prepare() {
	// Make pools to minimize heap usage:
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &HLQuery{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				MeasurementName:  make([]byte, 0, 1024),
				FieldName:        make([]byte, 0, 1024),
				AggregationType:  make([]byte, 0, 1024),
				TagsCondition:    make([]byte, 0, 1024),
			}
		},
	}

	b.session = NewCassandraSession(b.daemonUrl, b.requestTimeout)
	b.hlQueryChan = make(chan *HLQuery)
	b.queryExecutor = NewHLQueryExecutor(b.session, bulk_query.Benchmarker.Debug())
}

func (b *CassandraQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}

func (b *CassandraQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *CassandraQueryBenchmarker) CleanUp() {
	close(b.hlQueryChan)
	b.session.Close()

}

func (b *CassandraQueryBenchmarker) PrepareProcess(i int) {

}

func (b *CassandraQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}

func (b *CassandraQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	updatedTags = append(reportTags, [2]string{"aggregation_plan", b.aggrPlanLabel})
	updatedTags = append(updatedTags, [2]string{"subquery_workers", strconv.Itoa(b.subQueryParallelism)})
	updatedTags = append(updatedTags, [2]string{"request_timeout", strconv.Itoa(int(b.requestTimeout.Seconds()))})
	updatedTags = append(updatedTags, [2]string{"client_side_index_timeout", strconv.Itoa(int(b.csiTimeout.Seconds()))})

	params.DBType = "Cassandra"
	params.DestinationUrl = b.daemonUrl
	updatedExtraVals = extraVals
	return
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

// scan reads encoded Queries and places them onto the workqueue.
func (b *CassandraQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(r)

	n := int64(0)

loop:
	for {
		if bulk_query.Benchmarker.Limit() >= 0 && n >= bulk_query.Benchmarker.Limit() {
			break
		}

		q := b.queryPool.Get().(*HLQuery)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = n
		q.ForceUTC()

		b.hlQueryChan <- q

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

// processQueries reads byte buffers from hlQueryChan and writes them to the
// target server, while tracking latency.
func (b *CassandraQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	opts := HLQueryExecutorDoOptions{
		AggregationPlan:      b.aggrPlan,
		Debug:                bulk_query.Benchmarker.Debug(),
		PrettyPrintResponses: bulk_query.Benchmarker.PrettyPrintResponses(),
	}
	labels := map[string][][]byte{}
	for q := range b.hlQueryChan {
		qpLagMs, reqLagMs, err := b.queryExecutor.Do(q, opts)

		// if needed, prepare stat labels:
		if _, ok := labels[string(q.HumanLabel)]; !ok {
			labels[string(q.HumanLabel)] = [][]byte{
				q.HumanLabel,
				[]byte(fmt.Sprintf("%s-qp", q.HumanLabel)),
				[]byte(fmt.Sprintf("%s-req", q.HumanLabel)),
			}
		}
		ls := labels[string(q.HumanLabel)]

		// total lag stat:
		stat := statPool.Get().(*bulk_query.Stat)
		stat.InitWithActual(ls[0], qpLagMs+reqLagMs, true)
		statChan <- stat

		// qp lag stat:
		stat = statPool.Get().(*bulk_query.Stat)
		stat.InitWithActual(ls[1], qpLagMs, false)
		statChan <- stat

		// req lag stat:
		stat = statPool.Get().(*bulk_query.Stat)
		stat.InitWithActual(ls[2], reqLagMs, false)
		statChan <- stat

		b.queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}
