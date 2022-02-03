// query_benchmarker_timescale speed tests TimescaleDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided TimescaleDB endpoint using jackc/pgx.
//
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_query"

	"context"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type TimescaleQueryBenchmarker struct {
	// Program option vars:
	daemonUrl  string
	doQueries  bool
	psUser     string
	psPassword string
	// Global vars:
	queryPool    sync.Pool
	queryChan    chan []*Query
	hostPort     []string
	port         int
	scanFinished bool
	pool         *pgxpool.Pool
}

const DatabaseName = "benchmark_db"

var querier = &TimescaleQueryBenchmarker{}

// Parse args:
func init() {
	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()
}

func (b *TimescaleQueryBenchmarker) Init() {
	flag.StringVar(&b.daemonUrl, "url", "localhost:5432", "Daemon URL.")
	flag.StringVar(&b.psUser, "user", "postgres", "Postgresql user")
	flag.StringVar(&b.psPassword, "password", "", "Postgresql password")
	flag.BoolVar(&b.doQueries, "do-queries", true, "Whether to perform queries (useful for benchmarking the query executor.)")
}

func (b *TimescaleQueryBenchmarker) Validate() {
	var err error
	b.hostPort = strings.Split(b.daemonUrl, ":")
	if len(b.hostPort) != 2 {
		log.Fatalf("Invalid host:port '%s'", b.daemonUrl)
	}
	b.port, err = strconv.Atoi(b.hostPort[1])
	if err != nil {
		log.Fatalf("Invalid host:port '%s'", b.daemonUrl)
	}
}

func (b *TimescaleQueryBenchmarker) Prepare() {
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				QuerySQL:         make([]byte, 0, 1024),
			}
		},
	}
	b.queryChan = make(chan []*Query)
}

func (b *TimescaleQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}

func (b *TimescaleQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *TimescaleQueryBenchmarker) PrepareProcess(i int) {

}

func (b *TimescaleQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	var conn *pgx.Conn
	//var err error

	if b.doQueries {
		//# Example DSN
		//user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s database=%s", b.hostPort[0], uint16(b.port), b.psUser, b.psPassword, DatabaseName)
		fmt.Println("***** qyuery", dsn)
		config, err := pgx.ParseConfig(dsn)
		if err != nil {
			log.Fatal(err)
		}
		conn, err = pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}

	}
	func(connection *pgx.Conn) {
		if b.doQueries {
			defer connection.Close(context.Background())
		}
		b.processQueries(connection, workersGroup, statPool, statChan)
	}(conn)
}

func (b *TimescaleQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}
func (b *TimescaleQueryBenchmarker) CleanUp() {
	close(b.queryChan)
}

func (b *TimescaleQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	params.DBType = "TimescaleDB"
	params.DestinationUrl = b.daemonUrl
	updatedTags = reportTags
	updatedExtraVals = extraVals
	return
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

// scan reads encoded Queries and places them onto the workqueue.
func (b *TimescaleQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(bufio.NewReaderSize(r, 4*1024*1014))

	n := int64(0)
	bc := int64(0)
	batch := make([]*Query, 0, bulk_query.Benchmarker.BatchSize())
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
		batch = append(batch, q)

		bc++
		n++

		if bc == int64(bulk_query.Benchmarker.BatchSize()) {
			b.queryChan <- batch
			batch = batch[:0]
			bc = 0
		}
	}
	//make sure remaining batch goes out
	if bc > 0 {
		b.queryChan <- batch
	}
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func (b *TimescaleQueryBenchmarker) processQueries(conn *pgx.Conn, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	var lag float64
	var err error
	for qb := range b.queryChan {
		if len(qb) == 1 {
			lag, err = b.oneQuery(conn, qb[0])
			stat := statPool.Get().(*bulk_query.Stat)
			stat.Init(qb[0].HumanLabel, lag)
			statChan <- stat
			b.queryPool.Put(qb[0])
		} else {
			lag, err = b.batchQueries(conn, qb)
			lagPerQuery := lag / float64(len(qb))
			for _, q := range qb {
				stat := statPool.Get().(*bulk_query.Stat)
				stat.Init(q.HumanLabel, lagPerQuery)
				statChan <- stat
				b.queryPool.Put(q)
			}
		}

		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

// oneQuery executes on Query
func (b *TimescaleQueryBenchmarker) oneQuery(conn *pgx.Conn, q *Query) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	var timeCol int64
	var valCol float64
	if b.doQueries {
		rows, err := conn.Query(context.Background(), string(q.QuerySQL))
		if err != nil {
			log.Println("Error running query: '", string(q.QuerySQL), "'")
			return 0, err
		}
		for rows.Next() {
			if bulk_query.Benchmarker.PrettyPrintResponses() {
				rows.Scan(&timeCol, &valCol)
				t := time.Unix(0, timeCol).UTC()
				fmt.Printf("ID %d: %s, %f\n", q.ID, t, valCol)
			}
		}

		rows.Close()
	}

	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}

func (b *TimescaleQueryBenchmarker) batchQueries(conn *pgx.Conn, batch []*Query) (float64, error) {
	var err error

	start := time.Now().UnixNano()
	sqlBatch := pgx.Batch{}
	for _, query := range batch {
		sqlBatch.Queue(string(query.QuerySQL), nil, nil, []int16{pgx.BinaryFormatCode, pgx.BinaryFormatCode})
	}

	sqlBatchResults := b.pool.SendBatch(context.Background(), &sqlBatch)
	if err = sqlBatchResults.Close(); err != nil {
		log.Fatalf("failed to close a batch operation %v", err)
	}

	// Return the batch buffer to the pool.
	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}
