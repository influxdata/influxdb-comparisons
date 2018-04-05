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
	"os"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"context"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/jackc/pgx"
	"strconv"
	"strings"
)

// Program option vars:
var (
	daemonUrl            string
	workers              int
	debug                int
	prettyPrintResponses bool
	limit                int64
	burnIn               uint64
	printInterval        uint64
	memProfile           string
	doQueries            bool
	reportDatabase       string
	reportHost           string
	reportUser           string
	reportPassword       string
	reportTagsCSV        string
	psUser               string
	psPassword           string
	batchSize            int
)

// Global vars:
var (
	queryPool      sync.Pool
	queryChan      chan []*Query
	statPool       sync.Pool
	statChan       chan *Stat
	workersGroup   sync.WaitGroup
	statGroup      sync.WaitGroup
	statMapping    statsMap
	reportTags     [][2]string
	reportHostname string
)

type statsMap map[string]*StatGroup

const allQueriesLabel = "all queries"
const DatabaseName = "benchmark_db"

// Parse args:
func init() {

	flag.StringVar(&daemonUrl, "url", "localhost:5432", "Daemon URL.")
	flag.StringVar(&psUser, "user", "postgres", "Postgresql user")
	flag.StringVar(&psPassword, "password", "", "Postgresql password")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.IntVar(&batchSize, "batch-size", 1, "Batch size (input items).")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.BoolVar(&doQueries, "do-queries", true, "Whether to perform queries (useful for benchmarking the query executor.)")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics.")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics.")
	flag.StringVar(&reportUser, "report-user", "", "User for Host to send result metrics.")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics.")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics.")

	flag.Parse()

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
	var err error
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				QuerySQL:         make([]byte, 0, 1024),
			}
		},
	}

	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}

	// Make data and control channels:
	queryChan = make(chan []*Query, workers)
	statChan = make(chan *Stat, workers)

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats()

	hostPort := strings.Split(daemonUrl, ":")
	port, _ := strconv.Atoi(hostPort[1])
	// Launch the query processors:
	for i := 0; i < workers; i++ {
		var conn *pgx.Conn

		if doQueries {
			conn, err = pgx.Connect(pgx.ConnConfig{
				Host:     hostPort[0],
				Port:     uint16(port),
				User:     psUser,
				Password: psPassword,
				Database: DatabaseName,
			})
			if err != nil {
				log.Fatal(err)
			}

		}
		workersGroup.Add(1)
		go func(connection *pgx.Conn) {
			if doQueries {
				defer connection.Close()
			}
			processQueries(connection)
		}(conn)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	scan(input)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	workersGroup.Wait()
	close(statChan)

	// Wait on the stat collector to finish (and print its results):
	statGroup.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err = fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
	if reportHost != "" {

		reportParams := &report.QueryReportParams{
			ReportParams: report.ReportParams{
				DBType:             "TimescaleDB",
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
			BurnIn: int64(burnIn),
		}

		stat := statMapping[allQueriesLabel]
		err = report.ReportQueryResult(reportParams, stat.Min, stat.Mean, stat.Max, stat.Count, wallTook)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads encoded Queries and places them onto the workqueue.
func scan(r io.Reader) {
	dec := gob.NewDecoder(bufio.NewReaderSize(r, 4*1024*1014))

	n := int64(0)
	b := int64(0)
	batch := make([]*Query, 0, batchSize)
	for {
		if limit >= 0 && n >= limit {
			break
		}

		q := queryPool.Get().(*Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("decoder", err)
		}

		q.ID = n
		batch = append(batch, q)

		b++
		n++

		if b == int64(batchSize) {
			queryChan <- batch
			batch = batch[:0]
			b = 0
		}
	}
	//make sure remaining batch goes out
	if b > 0 {
		queryChan <- batch
	}
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(conn *pgx.Conn) {
	var lag float64
	var err error
	for qb := range queryChan {
		if len(qb) == 1 {
			lag, err = oneQuery(conn, qb[0])
			stat := statPool.Get().(*Stat)
			stat.Init(qb[0].HumanLabel, lag)
			statChan <- stat
			queryPool.Put(qb[0])
		} else {
			lag, err = batchQueries(conn, qb)
			lagPerQuery := lag / float64(len(qb))
			for _, q := range qb {
				stat := statPool.Get().(*Stat)
				stat.Init(q.HumanLabel, lagPerQuery)
				statChan <- stat
				queryPool.Put(q)
			}
		}

		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

// oneQuery executes on Query
func oneQuery(conn *pgx.Conn, q *Query) (float64, error) {
	start := time.Now().UnixNano()
	var err error
	var timeCol int64
	var valCol float64
	if doQueries {
		rows, err := conn.Query(string(q.QuerySQL))
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			if prettyPrintResponses {
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

func batchQueries(conn *pgx.Conn, batch []*Query) (float64, error) {
	var timeCol int64
	var valCol float64
	start := time.Now().UnixNano()
	sqlBatch := conn.BeginBatch()
	for _, query := range batch {
		sqlBatch.Queue(string(query.QuerySQL), nil, nil, []int16{pgx.BinaryFormatCode, pgx.BinaryFormatCode})
	}

	err := sqlBatch.Send(context.Background(), nil)

	if err != nil {
		log.Fatalf("Error writing: %s\n", err.Error())
	}

	for i := 0; i < len(batch); i++ {
		rows, err := sqlBatch.QueryResults()
		if err != nil {
			log.Fatalf("Error line %d of batch: %s\n", i, err.Error())
		}
		for rows.Next() {
			if prettyPrintResponses {
				err = rows.Scan(&timeCol, &valCol)
				if err != nil {
					log.Fatalf("Error scan row of query %d of batch: %s\n", i, err.Error())
				}
				t := time.Unix(0, timeCol).UTC()
				fmt.Printf("ID %d: %s, %f\n", batch[i].ID, t, valCol)
			}
		}

		rows.Close()

	}
	sqlBatch.Close()
	// Return the batch buffer to the pool.
	took := time.Now().UnixNano() - start
	lag := float64(took) / 1e6 // milliseconds
	return lag, err
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {
	statMapping = statsMap{
		allQueriesLabel: &StatGroup{},
	}

	i := uint64(0)
	for stat := range statChan {
		if i < burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == burnIn && burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}

		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = &StatGroup{}
		}

		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
			fprintStats(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-burnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	fprintStats(os.Stdout, statMapping)
	statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func fprintStats(w io.Writer, statGroups statsMap) {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		minRate := 1e3 / v.Min
		meanRate := 1e3 / v.Mean
		maxRate := 1e3 / v.Max
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
