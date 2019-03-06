// query_benchmarker speed tests Graphite using requests from stdin.
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
	graphiteUrl            string
	workers                int
	debug                  int
	prettyPrintResponses   bool
	limit                  int64
	burnIn                 uint64
	printInterval          uint64
	memProfile             string
	telemetryStderr        bool
	telemetryBatchSize     uint64
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
	gradualWorkersMax      int
	increaseInterval       time.Duration
	notificationHostPort   string
	dialTimeout            time.Duration
	readTimeout            time.Duration
	writeTimeout           time.Duration
	httpClientType         string
	trendSamples           int
	movingAverageInterval  time.Duration
	clientIndex            int
	scanFinished           bool
	reportTelemetry        bool
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
	isBurnIn            bool
)

type statsMap map[string]*StatGroup

const allQueriesLabel = "all queries"

// Parse args:
func init() {
	flag.StringVar(&graphiteUrl, "url", "http://localhost:8080", "Graphite URL.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 1, "Telemetry batch size (lines).")
	flag.BoolVar(&reportTelemetry, "report-telemetry", false, "Whether to report also progress info about mean, moving mean and #workers.")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics.")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics.")
	flag.StringVar(&reportUser, "report-user", "", "User for Host to send result metrics.")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics.")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics.")
	flag.StringVar(&useCase, "use-case", "", "Enables use-case specific behavior. Empty for default behavior. Additional use-cases: "+Dashboard)
	flag.IntVar(&queriesBatch, "batch-size", 18, "Number of queries in batch per worker for Dashboard use-case")
	flag.DurationVar(&waitInterval, "wait-interval", time.Second*0, "Delay between sending batches of queries in the dashboard use-case")
	flag.BoolVar(&gradualWorkersIncrease, "grad-workers-inc", false, "Whether to gradually increase number of workers. The 'workers' params defines initial number of workers in this case.")
	flag.IntVar(&gradualWorkersMax, "grad-workers-max", -1, "Maximum number of workers when are added gradually.")
	flag.DurationVar(&increaseInterval, "increase-interval", time.Second*30, "Interval when number of workers will increase")
	flag.DurationVar(&testDuration, "benchmark-duration", time.Second*0, "Run querying continually for defined time interval, instead of stopping after all queries have been used")
	flag.DurationVar(&responseTimeLimit, "response-time-limit", time.Second*0, "Query response time limit, after which will client stop.")
	flag.StringVar(&notificationHostPort, "notification-target", "", "host:port of finish message notification receiver")
	flag.DurationVar(&dialTimeout, "dial-timeout", time.Second*15, "TCP dial timeout.")
	flag.DurationVar(&readTimeout, "write-timeout", time.Second*300, "TCP write timeout.")
	flag.DurationVar(&writeTimeout, "read-timeout", time.Second*300, "TCP read timeout.")
	flag.StringVar(&httpClientType, "http-client-type", "fast", "HTTP client type {fast, default}")
	flag.IntVar(&trendSamples, "rt-trend-samples", -1, "Number of avg response time samples used for linear regression (-1: number of samples equals increase-interval in seconds)")
	flag.DurationVar(&movingAverageInterval, "moving-average-interval", time.Second*30, "Interval of measuring mean response time on which moving average  is calculated.")
	flag.IntVar(&clientIndex, "client-index", 0, "Index of a client host running this tool. Used to distribute load")

	flag.Parse()

	if workers < 1 {
		log.Fatalf("invalid number of workers: %d\n", workers)
	}

	batchSize = 1
	if useCase == Dashboard {
		batchSize = queriesBatch
		fmt.Printf("Dashboard simulation: %d batch, %s interval\n", batchSize, waitInterval)
	}
	if gradualWorkersIncrease {
		fmt.Printf("Gradual workers increasing in %s interval\n", increaseInterval)
		if gradualWorkersMax > 0 {
			fmt.Printf("Maximum number of gradual workers is %d\n", gradualWorkersMax)
		}
	}

	if testDuration.Nanoseconds() > 0 {
		fmt.Printf("Test will be run for %s\n", testDuration)
	}

	if responseTimeLimit.Nanoseconds() > 0 {
		fmt.Printf("Response time limit set to %s\n", responseTimeLimit)
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

	if reportTelemetry && reportHost == "" {
		log.Fatalf("invalid configuration: cannot report telemetry without specified report host")
	}

	if httpClientType == "fast" || httpClientType == "default" {
		fmt.Printf("Using HTTP client: %v\n", httpClientType)
		useFastHttp = httpClientType == "fast"
	} else {
		log.Fatalf("Unsupported HTPP client type: %v", httpClientType)
	}

	if trendSamples <= 0 {
		trendSamples = int(increaseInterval.Seconds())
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

	if movingAverageInterval <= 0 {
		movingAverageInterval = increaseInterval
	}
	movingAverageStat = NewTimedStatGroup(movingAverageInterval, trendSamples)
	fmt.Println("Reading queries to buffer ")
	queriesData, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("Error reading queries: %s", err)
	}
	fmt.Println("Reading queries done")

	qr := bytes.NewReader(queriesData)
	// Make data and control channels:
	queryChan = make(chan []*Query)
	statChanBuff := workers
	if gradualWorkersIncrease {
		if testDuration > 0 {
			statChanBuff = workers + int(testDuration.Seconds()/increaseInterval.Seconds())*workers
		} else {
			statChanBuff = workers * 11
		}

	}
	statChan = make(chan *Stat, statChanBuff)

	if reportTelemetry {
		telemetryCollector := report.NewCollector(reportHost, reportDatabase, reportUser, reportPassword)
		err = telemetryCollector.CreateDatabase()
		if err != nil {
			log.Fatalf("Error creating temetry db: %v\n", err)
		}
		telemetryChanPoints, telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, telemetryBatchSize, telemetryStderr, 0)
	}

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats(telemetryChanPoints)

	workersIncreaseStep := workers
	// Launch the query processors:
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		w := NewHTTPClient(graphiteUrl, debug, dialTimeout, readTimeout, writeTimeout)
		go processQueries(w)
	}
	log.Printf("Started querying with %d workers\n", workers)

	wallStart := time.Now()

	scanRes := make(chan int)
	scanClose := make(chan int)
	responseTimeLimitReached := false
	timeoutReached := false
	timeLimit := testDuration.Nanoseconds() > 0
	go func() {
		for {
			scan(qr, scanClose)
			cont := !(responseTimeLimit.Nanoseconds() > 0 && responseTimeLimitReached) && timeLimit && !timeoutReached
			//log.Printf("Scan done, should continue: %v, responseTimeLimit: %d, responseTimeLimitReached: %v, testDuration: %d, timeoutcheck %v", cont, responseTimeLimit, responseTimeLimitReached, testDuration, time.Now().Before(wallStart.Add(testDuration)))
			if cont {
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

	if !timeLimit {
		//we need a time limit to have timer set, so set some  long time
		testDuration = time.Hour * 24
	}
	tickerQuaters := 0
	timeoutTicker := time.NewTicker(testDuration / 4)
	reponseTimeLimitWorkers := 0

loop:
	for {
		select {
		case <-scanRes:
			break loop
		case <-workersTicker.C:
			if gradualWorkersIncrease && !isBurnIn {
				if gradualWorkersMax <= 0 || workers <= gradualWorkersMax {
					for i := 0; i < workersIncreaseStep; i++ {
						//fmt.Printf("Adding worker %d\n", workers)
						workersGroup.Add(1)
						w := NewHTTPClient(graphiteUrl, debug, dialTimeout, readTimeout, writeTimeout)
						go processQueries(w)
						workers++
					}
					log.Printf("Added %d workers, total: %d\n", workersIncreaseStep, workers)
				} else {
					log.Printf("Maximum %d workers already reached: %d\n", gradualWorkersMax, workers)
				}
			}
		case <-responseTicker.C:
			if !responseTimeLimitReached && responseTimeLimit > 0 && responseTimeLimit.Nanoseconds()*3 < int64(movingAverageStat.Avg()*1e6) {
				responseTimeLimitReached = true
				scanClose <- 1
				respLimitms := float64(responseTimeLimit.Nanoseconds()) / 1e6
				item := movingAverageStat.FindHistoryItemBelow(respLimitms)
				if item == nil {
					log.Printf("Couln't find reponse time limit %.2f, maybe it's too low\n", respLimitms)
					reponseTimeLimitWorkers = workers
				} else {
					log.Printf("Mean response time reached threshold: %.2fms > %.2fms, with %d workers\n", item.value, respLimitms, item.item)
					reponseTimeLimitWorkers = item.item
				}
			}
		case <-timeoutTicker.C:
			tickerQuaters++
			if gradualWorkersIncrease && !responseTimeLimitReached && responseTimeLimit > 0 && !timeoutReached && tickerQuaters > 1 {
				//if we didn't reached response time limit in 50% of time limit, double workers increase step
				workersIncreaseStep = 2 * workersIncreaseStep
				log.Printf("Response time limit has not reached yet. Increasing workers increase step 2x to %d\n", workersIncreaseStep)
			}
			if timeLimit && tickerQuaters > 3 && !timeoutReached {
				timeoutReached = true
				log.Println("Time out reached")
				if !scanFinished {
					scanClose <- 1
				} else {
					log.Println("Scan already finished")
				}
				if responseTimeLimit > 0 {
					//still try to find response time limit
					respLimitms := float64(responseTimeLimit.Nanoseconds()) / 1e6
					item := movingAverageStat.FindHistoryItemBelow(respLimitms)
					if item == nil {
						log.Printf("Couln't find reponse time limit %.2f, maybe it's too low\n", respLimitms)
						reponseTimeLimitWorkers = workers
					} else {
						log.Printf("Mean response time reached threshold: %.2fms > %.2fms, with %d workers\n", item.value, respLimitms, item.item)
						reponseTimeLimitWorkers = item.item
					}

				}
			}

		}

	}

	close(scanClose)
	close(scanRes)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	log.Println("Waiting for workers to finish")
	waitCh := make(chan int)
	waitFinished := false
	go func() {
		workersGroup.Wait()
		waitFinished = true
		waitCh <- 1
	}()
	waitTimer := time.NewTimer(time.Minute * 10)
waitLoop:
	for {
		select {
		case <-waitCh:
			waitTimer.Stop()
			break waitLoop
		case <-waitTimer.C:
			log.Println("Waiting for workers timeout")
			break waitLoop
		}
	}
	close(waitCh)

	close(statChan)

	// Wait on the stat collector to finish (and print its results):
	statGroup.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err = fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	if reportTelemetry {
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
		extraVals := make([]report.ExtraVal, 0, 1)
		reportTags = append(reportTags, [2]string{"batch_size", fmt.Sprintf("%d", batchSize)})
		reportTags = append(reportTags, [2]string{"wait_interval", waitInterval.String()})
		reportTags = append(reportTags, [2]string{"grad_workers_inc", fmt.Sprintf("%v", gradualWorkersIncrease)})
		reportTags = append(reportTags, [2]string{"increase_interval", increaseInterval.String()})
		reportTags = append(reportTags, [2]string{"benchmark_duration", testDuration.String()})
		reportTags = append(reportTags, [2]string{"response_time_limit", responseTimeLimit.String()})
		if responseTimeLimitReached {
			reportTags = append(reportTags, [2]string{"response_time_limit_reached", fmt.Sprintf("%v", responseTimeLimitReached)})
			extraVals = append(extraVals, report.ExtraVal{Name: "response_time_limit_workers", Value: int64(reponseTimeLimitWorkers)})
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
				DestinationUrl:     graphiteUrl,
				Workers:            workers,
				ItemLimit:          int(limit),
			},
			BurnIn: int64(burnIn),
		}

		if len(statMapping) > 2 {
			for query, stat := range statMapping {
				err = report.ReportQueryResult(reportParams, query, stat.Min, stat.Mean, stat.Max, stat.Count, wallTook, extraVals...)
				if err != nil {
					log.Fatal(err)
				}
			}
		} else {
			stat := statMapping[allQueriesLabel]
			err = report.ReportQueryResult(reportParams, allQueriesLabel, stat.Min, stat.Mean, stat.Max, stat.Count, wallTook, extraVals...)
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
	scanFinished = true
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(w HTTPClient) error {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
	}
	var queriesSeen int64
	for queries := range queryChan {
		if len(queries) == 1 {
			if err := processSingleQuery(w, queries[0], opts, nil, nil); err != nil {
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
				go processSingleQuery(w, q, opts, errCh, doneCh)
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

func processSingleQuery(w HTTPClient, q *Query, opts *HTTPClientDoOptions, errCh chan error, doneCh chan int) error {
	defer func() {
		if doneCh != nil {
			doneCh <- 1
		}
	}()
	lagMillis, respSize, err := w.Do(q, opts)
	stat := statPool.Get().(*Stat)
	stat.Init(q.HumanLabel, lagMillis)
	statChan <- stat
	queryPool.Put(q)
	log.Printf("response size = %d", respSize)
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

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats(telemetrySink chan *report.Point) {

	statMapping = statsMap{
		allQueriesLabel: &StatGroup{},
	}

	lastRefresh := time.Time{}
	i := uint64(0)
	for stat := range statChan {
		isBurnIn = i < burnIn
		if isBurnIn {
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

		now := time.Now()

		movingAverageStat.Push(now, stat.Value)
		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		if lastRefresh.Nanosecond() == 0 || now.Sub(lastRefresh).Seconds() >= 1.0 {
			movingAverageStat.UpdateAvg(now, workers)
			lastRefresh = now
			// Report telemetry, if applicable:
			if telemetrySink != nil {
				p := report.GetPointFromGlobalPool()
				p.Init("benchmarks_telemetry", now.UnixNano())
				for _, tagpair := range reportTags {
					p.AddTag(tagpair[0], tagpair[1])
				}
				p.AddTag("client_type", "query")
				p.AddFloat64Field("query_response_time_mean", statMapping[allQueriesLabel].Mean)
				p.AddFloat64Field("query_response_time_moving_mean", movingAverageStat.Avg())
				p.AddIntField("query_workers", workers)
				p.AddInt64Field("queries", int64(i))
				telemetrySink <- p
			}
		}
		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "%s: after %d queries with %d workers:\n", time.Now().String(), i-burnIn, workers)
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
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), moving mean: %8.2fms, moving median: %8.2fms, max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, movingAverageStat.Avg(), movingAverageStat.Median(), v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}
}
