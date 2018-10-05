// query_benchmarker speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"bytes"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"io/ioutil"
)

const Dashboard = "dashboard"

// Program option vars:
var (
	csvDaemonUrls          string
	daemonUrls             []string
	workers                int
	debug                  int
	prettyPrintResponses   bool
	limit                  int64
	burnIn                 uint64
	printInterval          uint64
	memProfile             string
	telemetryHost          string
	telemetryStderr        bool
	telemetryBatchSize     uint64
	telemetryTagsCSV       string
	telemetryBasicAuth     string
	reportDatabase         string
	reportHost             string
	reportUser             string
	reportPassword         string
	reportTagsCSV          string
	useCase                string
	queriesBatch           int
	waitInterval           time.Duration
	responseTimeLimit      time.Duration
	testDuration           time.Duration
	gradualWorkersIncrease bool
	increaseInterval       time.Duration
	notificationHostPort   string
	dialTimeout            time.Duration
)

// Global vars:
var (
	queryPool           sync.Pool
	queryChan           chan []*Query
	statPool            sync.Pool
	statChan            chan *Stat
	workersGroup        sync.WaitGroup
	statGroup           sync.WaitGroup
	telemetryChanPoints chan *report.Point
	telemetryChanDone   chan struct{}
	telemetrySrcAddr    string
	telemetryTags       [][2]string
	statMapping         statsMap
	reportTags          [][2]string
	reportHostname      string
	batchSize           int
	movingAverageStat   *TimedStatGroup
)

type statsMap map[string]*StatGroup

const allQueriesLabel = "all queries"

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.StringVar(&telemetryTagsCSV, "telemetry-tags", "", "Tag(s) for telemetry. Format: key0:val0,key1:val1,...")
	flag.StringVar(&telemetryBasicAuth, "telemetry-basic-auth", "", "basic auth (username:password) for telemetry.")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 1000, "Telemetry batch size (lines).")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics.")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics.")
	flag.StringVar(&reportUser, "report-user", "", "User for Host to send result metrics.")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics.")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics.")
	flag.StringVar(&useCase, "use-case", "", "Enables use-case specific behavior. Empty for default behavior. Additional use-cases: "+Dashboard)
	flag.IntVar(&queriesBatch, "batch-size", 18, "Number of queries in batch per worker for Dashboard use-case")
	flag.DurationVar(&waitInterval, "wait-interval", time.Second*0, "Delay between sending batches of queries in the dashboard use-case")
	flag.BoolVar(&gradualWorkersIncrease, "grad-workers-inc", false, "Whether to gradually increase number of workers. The 'workers' params defines initial number of workers in this case.")
	flag.DurationVar(&increaseInterval, "increase-interval", time.Second*30, "Interval when number of workers will increase")
	flag.DurationVar(&testDuration, "benchmark-duration", time.Second*0, "Run querying continually for defined time interval, instead of stopping after all queries have been used")
	flag.DurationVar(&responseTimeLimit, "response-time-limit", time.Second*0, "Query response time limit, after which will client stop.")
	flag.StringVar(&notificationHostPort, "notification-target", "", "host:port of finish message notification receiver")
	flag.DurationVar(&dialTimeout, "dial-timeout", time.Second*15, "TCP dial timeout.")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

	batchSize = 1
	if useCase == Dashboard {
		batchSize = queriesBatch
		fmt.Printf("Dashboard simulation: %d batch, %s interval\n", batchSize, waitInterval)
	}
	if gradualWorkersIncrease {
		fmt.Printf("Gradual workers increasing in %s interval\n", increaseInterval)
	}

	if testDuration.Nanoseconds() > 0 {
		fmt.Printf("Test will be run for %s\n", testDuration)
	}

	if responseTimeLimit.Nanoseconds() > 0 {
		fmt.Printf("Response time limit set to %s\n", responseTimeLimit)
	}

	if telemetryHost != "" {
		fmt.Printf("telemetry destination: %v\n", telemetryHost)
		if telemetryBatchSize == 0 {
			panic("invalid telemetryBatchSize")
		}

		var err error
		telemetrySrcAddr, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("src addr for telemetry: %v\n", telemetrySrcAddr)

		if telemetryTagsCSV != "" {
			pairs := strings.Split(telemetryTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				telemetryTags = append(telemetryTags, tagpair)
			}
		}
		fmt.Printf("telemetry tags: %v\n", telemetryTags)
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
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
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

	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}
	movingAverageStat = NewTimedStatGroup(increaseInterval)
	fmt.Println("Reading queries to buffer ")
	queriesData, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Error reading queries: %s", err)
	}
	fmt.Println("Reading queries done")

	qr := bytes.NewReader(queriesData)
	// Make data and control channels:
	queryChan = make(chan []*Query, workers)
	statChan = make(chan *Stat, workers)

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats()

	if telemetryHost != "" {
		telemetryCollector := report.NewCollector(telemetryHost, "telegraf", telemetryBasicAuth)
		telemetryChanPoints, telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, telemetryBatchSize, telemetryStderr, burnIn)
	}

	workersIncreaseStep := workers
	// Launch the query processors:
	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		w := NewHTTPClient(daemonUrl, debug, dialTimeout)
		go processQueries(w, telemetryChanPoints, fmt.Sprintf("%d", i))
	}

	wallStart := time.Now()

	scanRes := make(chan int)
	scanClose := make(chan int)
	responseTimeLimitReached := false
	go func() {
		for {
			scan(qr, scanClose)
			if !(responseTimeLimit.Nanoseconds() > 0 && responseTimeLimitReached) && testDuration.Nanoseconds() > 0 && time.Now().Before(wallStart.Add(testDuration)) {
				qr = bytes.NewReader(queriesData)
			} else {
				scanRes <- 1
				break
			}
		}
	}()

	workersTicker := time.NewTicker(increaseInterval)
	defer workersTicker.Stop()
	responseTicker := time.NewTicker(time.Second)
	defer responseTicker.Stop()

loop:
	for {
		select {
		case <-scanRes:
			break loop
		case <-workersTicker.C:
			if gradualWorkersIncrease {
				for i := 0; i < workersIncreaseStep; i++ {
					fmt.Printf("Adding worker %d\n", workers)
					daemonUrl := daemonUrls[workers%len(daemonUrls)]
					workersGroup.Add(1)
					w := NewHTTPClient(daemonUrl, debug, dialTimeout)
					go processQueries(w, telemetryChanPoints, fmt.Sprintf("%d", workers))
					workers++
				}
			}
		case <-responseTicker.C:
			if responseTimeLimit.Nanoseconds() > 0 && responseTimeLimit.Nanoseconds() < int64(movingAverageStat.Avg()*1e6) && statMapping[allQueriesLabel].Count > 1000 {
				responseTimeLimitReached = true
				fmt.Printf("Mean response time is above threshold: %.2fms > %.2fms\n", movingAverageStat.Avg(), float64(responseTimeLimit.Nanoseconds())/1e6)
				scanClose <- 1
			}
		}
	}

	close(scanClose)
	close(scanRes)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	fmt.Println("Waiting for workers to finish")
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
	if gradualWorkersIncrease {
		fmt.Printf("Final workers count: %d", workers)
	}

	if telemetryHost != "" {
		fmt.Println("shutting down telemetry...")
		close(telemetryChanPoints)
		<-telemetryChanDone
		fmt.Println("done shutting down telemetry.")
	}

	if notificationHostPort != "" {
		client, err := rpc.DialHTTP("tcp", notificationHostPort)
		if err != nil {
			log.Println("error: dialing:", err)
		} else {
			var res int
			input := 0
			call := client.Go("NotifyReceiver.Notify", input, &res, nil)
			if call.Error != nil {
				log.Println("error: calling:", call.Error)
			}
		}
	}

	if reportHost != "" {
		found := false
		for _, pair := range reportTags {
			if pair[0] == "use_case" {
				found = true
				break
			}
		}
		if useCase != "" && !found {
			reportTags = append(reportTags, [2]string{"use_case", useCase})
		}
		reportTags = append(reportTags, [2]string{"batch_size", fmt.Sprintf("%d", batchSize)})
		reportTags = append(reportTags, [2]string{"wait_interval", waitInterval.String()})
		reportTags = append(reportTags, [2]string{"grad_workers_inc", fmt.Sprintf("%v", gradualWorkersIncrease)})
		reportTags = append(reportTags, [2]string{"increase_interval", increaseInterval.String()})
		reportTags = append(reportTags, [2]string{"benchmark_duration", testDuration.String()})
		reportTags = append(reportTags, [2]string{"response_time_limit", responseTimeLimit.String()})
		if responseTimeLimitReached {
			reportTags = append(reportTags, [2]string{"response_time_limit_reached", fmt.Sprintf("%v", responseTimeLimitReached)})
		}

		reportParams := &report.QueryReportParams{
			ReportParams: report.ReportParams{
				DBType:             "InfluxDB",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     csvDaemonUrls,
				Workers:            workers,
				ItemLimit:          int(limit),
			},
			BurnIn: int64(burnIn),
		}
		if len(statMapping) > 2 {
			for query, stat := range statMapping {
				movingAvg := float64(-1)
				if query == allQueriesLabel {
					movingAvg = movingAverageStat.Avg()
				}
				err = report.ReportQueryResult(reportParams, query, stat.Min, stat.Mean, stat.Max, stat.Count, movingAvg, wallTook)
				if err != nil {
					log.Fatal(err)
				}
			}
		} else {
			stat := statMapping[allQueriesLabel]
			err = report.ReportQueryResult(reportParams, allQueriesLabel, stat.Min, stat.Mean, stat.Max, stat.Count, movingAverageStat.Avg(), wallTook)
			if err != nil {
				log.Fatal(err)
			}
		}

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
}

var qind int64

// scan reads encoded Queries and places them onto the workqueue.
func scan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(r)

	batch := make([]*Query, 0, batchSize)

	i := 0
loop:
	for {
		if limit >= 0 && qind >= limit {
			break
		}

		q := queryPool.Get().(*Query)
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
		if i == batchSize {
			queryChan <- batch
			//batch = batch[:0]
			batch = nil
			batch = make([]*Query, 0, batchSize)
			i = 0
		}

		qind++
		select {
		case <-closeChan:
			fmt.Printf("Received finish request\n")
			break loop
		default:
		}

	}
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(w *HTTPClient, telemetrySink chan *report.Point, telemetryWorkerLabel string) error {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
	}
	var queriesSeen int64
	for queries := range queryChan {
		if len(queries) == 1 {
			if err := processSingleQuery(w, queries[0], opts, telemetrySink, telemetryWorkerLabel, queriesSeen, nil, nil); err != nil {
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
				go processSingleQuery(w, q, opts, telemetrySink, telemetryWorkerLabel, queriesSeen, errCh, doneCh)
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
		if waitInterval.Seconds() > 0 {
			time.Sleep(waitInterval)
		}
	}
	workersGroup.Done()
	return nil
}

func processSingleQuery(w *HTTPClient, q *Query, opts *HTTPClientDoOptions, telemetrySink chan *report.Point, telemetryWorkerLabel string, queriesSeen int64, errCh chan error, doneCh chan int) error {
	defer func() {
		if doneCh != nil {
			doneCh <- 1
		}
	}()
	ts := time.Now().UnixNano()
	lagMillis, err := w.Do(q, opts)
	stat := statPool.Get().(*Stat)
	stat.Init(q.HumanLabel, lagMillis)
	statChan <- stat
	queryPool.Put(q)
	if err != nil {
		qerr := fmt.Errorf("Error during request of query %s: %s\n", q.String(), err.Error())
		if errCh != nil {
			errCh <- qerr
			return nil
		} else {
			return qerr
		}
	}
	// Report telemetry, if applicable:
	if telemetrySink != nil {
		p := report.GetPointFromGlobalPool()
		p.Init("benchmark_query", ts)
		for _, tagpair := range telemetryTags {
			p.AddTag(tagpair[0], tagpair[1])
		}
		p.AddTag("src_addr", telemetrySrcAddr)
		p.AddTag("dst_addr", w.HostString)
		p.AddTag("worker_id", telemetryWorkerLabel)
		p.AddFloat64Field("rtt_ms", lagMillis)
		p.AddInt64Field("worker_req_num", queriesSeen)
		telemetrySink <- p
	}
	return nil
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {

	statMapping = statsMap{
		allQueriesLabel: &StatGroup{},
	}

	lastRefresh := time.Time{}
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

		movingAverageStat.Push(time.Now(), stat.Value)
		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		if lastRefresh.Second() == 0 || time.Now().Sub(lastRefresh).Seconds() > 1 {
			movingAverageStat.UpdateAvg()
			lastRefresh = time.Now()
		}
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
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), moving mean: %8.2fms, max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, movingAverageStat.Avg(), v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
