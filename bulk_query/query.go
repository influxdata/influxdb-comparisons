package bulk_query

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
)

type BulkQuery interface {
	Init()
	Validate()
	Prepare()
	GetProcessor() Processor
	GetScanner() Scanner
	CleanUp()
	UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) ([][2]string, []report.ExtraVal)
}

type QueryBenchmarker struct {
	//program options
	debug                  int
	workers                int
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
	notificationGroup      string
	notificationListenPort int
	trendSamples           int
	movingAverageInterval  time.Duration
	reportTelemetry        bool
	file                   string
	//runtime vars
	statMapping       StatsMap
	statChan          chan *Stat
	statPool          sync.Pool
	statGroup         sync.WaitGroup
	telemetrySrcAddr  string
	telemetryTags     [][2]string
	reportTags        [][2]string
	reportHostname    string
	batchSize         int
	movingAverageStat *TimedStatGroup
	isBurnIn          bool
	sourceReader      *os.File
	scanClose         chan int
	scanCloseMutex    *sync.Mutex
	notificationServer *http.Server
	sigtermReceived   bool
}

const (
	Dashboard       = "dashboard"
	AllQueriesLabel = "all queries"
)

var Benchmarker = &QueryBenchmarker{}

func (q QueryBenchmarker) Debug() int {
	return q.debug
}

func (q QueryBenchmarker) Limit() int64 {
	return q.limit
}

func (q QueryBenchmarker) BatchSize() int {
	return q.batchSize
}

func (q QueryBenchmarker) GradualWorkersIncrease() bool {
	return q.gradualWorkersIncrease
}

func (q QueryBenchmarker) PrettyPrintResponses() bool {
	return q.prettyPrintResponses
}

func (q QueryBenchmarker) WaitInterval() time.Duration {
	return q.waitInterval
}

func (q *QueryBenchmarker) Init() {
	flag.StringVar(&q.useCase, "use-case", "", "Enables use-case specific behavior. Empty for default behavior. Additional use-cases: "+Dashboard)
	flag.IntVar(&q.workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&q.debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&q.limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&q.burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&q.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&q.prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&q.memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.BoolVar(&q.telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&q.telemetryBatchSize, "telemetry-batch-size", 1, "Telemetry batch size (lines).")
	flag.BoolVar(&q.reportTelemetry, "report-telemetry", false, "Whether to report also progress info about mean, moving mean and #workers.")
	flag.StringVar(&q.reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics.")
	flag.StringVar(&q.reportHost, "report-host", "", "Host to send result metrics.")
	flag.StringVar(&q.reportUser, "report-user", "", "User for Host to send result metrics.")
	flag.StringVar(&q.reportPassword, "report-password", "", "User password for Host to send result metrics.")
	flag.StringVar(&q.reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics.")
	flag.IntVar(&q.queriesBatch, "batch-size", 18, "Number of queries in batch per worker for Dashboard use-case")
	flag.DurationVar(&q.waitInterval, "wait-interval", time.Second*0, "Delay between sending batches of queries in the dashboard use-case")
	flag.BoolVar(&q.gradualWorkersIncrease, "grad-workers-inc", false, "Whether to gradually increase number of workers. The 'workers' params defines initial number of workers in this case.")
	flag.IntVar(&q.gradualWorkersMax, "grad-workers-max", -1, "Maximum number of workers when are added gradually.")
	flag.DurationVar(&q.increaseInterval, "increase-interval", time.Second*30, "Interval when number of workers will increase")
	flag.DurationVar(&q.testDuration, "benchmark-duration", time.Second*0, "Run querying continually for defined time interval, instead of stopping after all queries have been used")
	flag.DurationVar(&q.responseTimeLimit, "response-time-limit", time.Second*0, "Query response time limit, after which will client stop.")
	flag.StringVar(&q.notificationHostPort, "notification-target", "", "host:port of finish message notification receiver")
	flag.StringVar(&q.notificationGroup, "notification-group", "", "Terminate message notification siblings (comma-separated host:port list of other query benchmarkers)")
	flag.IntVar(&q.notificationListenPort, "notification-port", -1, "Listen port for remote notification messages. Used to remotely terminate benchmark (use -1 to disable it)")
	flag.IntVar(&q.trendSamples, "rt-trend-samples", -1, "Number of avg response time samples used for linear regression (-1: number of samples equals increase-interval in seconds)")
	flag.DurationVar(&q.movingAverageInterval, "moving-average-interval", time.Second*30, "Interval of measuring mean response time on which moving average  is calculated.")
	flag.StringVar(&q.file, "file", "", "Input file")
}

func (q *QueryBenchmarker) Validate() {
	if q.workers < 1 {
		log.Fatalf("invalid number of workers: %d\n", q.workers)
	}

	q.batchSize = 1
	if q.useCase == Dashboard {
		q.batchSize = q.queriesBatch
		fmt.Printf("Dashboard simulation: %d batch, %s interval\n", q.batchSize, q.waitInterval)
	}
	if q.gradualWorkersIncrease {
		fmt.Printf("Gradual workers increasing in %s interval\n", q.increaseInterval)
		if q.gradualWorkersMax > 0 {
			fmt.Printf("Maximum number of gradual workers is %d\n", q.gradualWorkersMax)
		}
	}

	if q.testDuration.Nanoseconds() > 0 {
		fmt.Printf("Test will be run for %s\n", q.testDuration)
	}

	if q.responseTimeLimit.Nanoseconds() > 0 {
		fmt.Printf("Response time limit set to %s\n", q.responseTimeLimit)
	}

	if q.reportHost != "" {
		fmt.Printf("results report destination: %v\n", q.reportHost)
		fmt.Printf("results report database: %v\n", q.reportDatabase)

		var err error
		q.reportHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("hostname for results report: %v\n", q.reportHostname)

		if q.reportTagsCSV != "" {
			pairs := strings.Split(q.reportTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				q.reportTags = append(q.reportTags, tagpair)
			}
		}
		fmt.Printf("results report tags: %v\n", q.reportTags)
	}

	if q.reportTelemetry && q.reportHost == "" {
		log.Fatalf("invalid configuration: cannot report telemetry without specified report host")
	}

	if q.trendSamples <= 0 {
		q.trendSamples = int(q.increaseInterval.Seconds())
	}

	if q.file != "" {
		if f, err := os.Open(q.file); err == nil {
			q.sourceReader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", q.file, err)
		}
	}
	if q.sourceReader == nil {
		q.sourceReader = os.Stdin
	}

	if q.notificationHostPort != "" {
		fmt.Printf("notification target: %v\n", q.notificationHostPort)
	}
	if q.notificationGroup != "" {
		fmt.Printf("notification group: %v\n", q.notificationGroup)
	}
	if q.notificationListenPort > 0 { // copied from bulk_load_influx/main.go
		notif := new(bulk_load.NotifyReceiver)
		rpc.Register(notif)
		rpc.HandleHTTP()
		bulk_load.RegisterHandler(q.notifyHandler)
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", q.notificationListenPort))
		if e != nil {
			log.Fatal("listen error:", e)
		}
		log.Println("Listening for incoming notification")
		q.notificationServer = &http.Server{}
		go q.notificationServer.Serve(l)
	}
}

func (q *QueryBenchmarker) RunBenchmark(bulkQuery BulkQuery) {
	q.statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}

	var workersGroup sync.WaitGroup

	if q.movingAverageInterval <= 0 {
		q.movingAverageInterval = q.increaseInterval
	}
	q.movingAverageStat = NewTimedStatGroup(q.movingAverageInterval, q.trendSamples)
	fmt.Println("Reading queries to buffer ")
	input := bufio.NewReaderSize(q.sourceReader, 1<<20)
	queriesData, err := ioutil.ReadAll(input)
	if err != nil {
		log.Fatalf("Error reading queries: %s", err)
	}
	fmt.Println("Reading queries done")

	qr := bytes.NewReader(queriesData)
	// Make data and control channels:
	statChanBuff := q.workers
	if q.gradualWorkersIncrease {
		if q.testDuration > 0 {
			statChanBuff = q.workers + int(q.testDuration.Seconds()/q.increaseInterval.Seconds())*q.workers
		} else {
			statChanBuff = q.workers * 11
		}

	}
	q.statChan = make(chan *Stat, statChanBuff)

	var telemetryChanPoints chan *report.Point
	var telemetryChanDone chan struct{}

	if q.reportTelemetry {
		telemetryCollector := report.NewCollector(q.reportHost, q.reportDatabase, q.reportUser, q.reportPassword)
		err = telemetryCollector.CreateDatabase()
		if err != nil {
			log.Fatalf("Error creating temetry db: %v\n", err)
		}
		telemetryChanPoints, telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, q.telemetryBatchSize, q.telemetryStderr, 0)
	}

	// Launch the stats processor:
	q.statGroup.Add(1)
	go q.processStats(telemetryChanPoints)

	bulkQuery.Prepare()

	processor := bulkQuery.GetProcessor()

	workersIncreaseStep := q.workers
	// Launch the query processors:
	for i := 0; i < q.workers; i++ {
		workersGroup.Add(1)
		processor.PrepareProcess(i)
		go processor.RunProcess(i, &workersGroup, q.statPool, q.statChan)
	}
	log.Printf("Started querying with %d workers\n", q.workers)

	wallStart := time.Now()

	scanRes := make(chan int, 1)
	q.scanCloseMutex = &sync.Mutex{}
	q.scanClose = make(chan int, 1)
	responseTimeLimitReached := false
	timeoutReached := false
	timeLimit := q.testDuration.Nanoseconds() > 0
	scanner := bulkQuery.GetScanner()
	go func() {
		for {
			scanner.RunScan(qr, q.scanClose)
			cont := !(q.responseTimeLimit.Nanoseconds() > 0 && responseTimeLimitReached) && timeLimit && !timeoutReached && !q.sigtermReceived
			//log.Printf("Scan done, should continue: %v, responseTimeLimit: %d, responseTimeLimitReached: %v, testDuration: %d, timeoutcheck %v", cont, responseTimeLimit, responseTimeLimitReached, testDuration, time.Now().Before(wallStart.Add(testDuration)))
			if cont {
				qr = bytes.NewReader(queriesData)
			} else {
				scanRes <- 1
				break
			}
		}
	}()

	workersTicker := time.NewTicker(q.increaseInterval)
	defer workersTicker.Stop()
	responseTicker := time.NewTicker(time.Second)
	defer responseTicker.Stop()

	if !timeLimit {
		//we need a time limit to have timer set, so set some  long time
		q.testDuration = time.Hour * 24
	}
	tickerQuaters := 0
	timeoutTicker := time.NewTicker(q.testDuration / 4)
	reponseTimeLimitWorkers := 0

loop:
	for {
		select {
		case <-scanRes:
			break loop
		case <-workersTicker.C:
			if q.gradualWorkersIncrease && !q.isBurnIn {
				if q.gradualWorkersMax <= 0 || q.workers <= q.gradualWorkersMax {
					for i := 0; i < workersIncreaseStep; i++ {
						//fmt.Printf("Adding worker %d\n", workers)
						workersGroup.Add(1)
						processor.PrepareProcess(q.workers)
						go processor.RunProcess(q.workers, &workersGroup, q.statPool, q.statChan)
						q.workers++
					}
					log.Printf("Added %d workers, total: %d\n", workersIncreaseStep, q.workers)
				} else {
					log.Printf("Maximum %d workers already reached: %d\n", q.gradualWorkersMax, q.workers)
				}
			}
		case <-responseTicker.C:
			if !responseTimeLimitReached && q.responseTimeLimit > 0 && q.responseTimeLimit.Nanoseconds()*3 < int64(q.movingAverageStat.Avg()*1e6) {
				responseTimeLimitReached = true
				q.stopScan()
				respLimitms := float64(q.responseTimeLimit.Nanoseconds()) / 1e6
				item := q.movingAverageStat.FindHistoryItemBelow(respLimitms)
				if item == nil {
					log.Printf("Couln't find reponse time limit %.2f, maybe it's too low\n", respLimitms)
					reponseTimeLimitWorkers = q.workers
				} else {
					log.Printf("Mean response time reached threshold: %.2fms > %.2fms, with %d workers\n", item.Value, respLimitms, item.Item)
					reponseTimeLimitWorkers = item.Item
				}
			}
		case <-timeoutTicker.C:
			tickerQuaters++
			if q.gradualWorkersIncrease && !responseTimeLimitReached && q.responseTimeLimit > 0 && !timeoutReached && tickerQuaters > 1 {
				//if we didn't reached response time limit in 50% of time limit, double workers increase step
				workersIncreaseStep = 2 * workersIncreaseStep
				log.Printf("Response time limit has not reached yet. Increasing workers increase step 2x to %d\n", workersIncreaseStep)
			}
			if timeLimit && tickerQuaters > 3 && !timeoutReached {
				timeoutReached = true
				log.Println("Time out reached")
				q.stopScan()
				if q.responseTimeLimit > 0 {
					//still try to find response time limit
					respLimitms := float64(q.responseTimeLimit.Nanoseconds()) / 1e6
					item := q.movingAverageStat.FindHistoryItemBelow(respLimitms)
					if item == nil {
						log.Printf("Couln't find reponse time limit %.2f, maybe it's too low\n", respLimitms)
						reponseTimeLimitWorkers = q.workers
					} else {
						log.Printf("Mean response time reached threshold: %.2fms > %.2fms, with %d workers\n", item.Value, respLimitms, item.Item)
						reponseTimeLimitWorkers = item.Item
					}

				}
			}

		}

	}

	q.scanCloseMutex.Lock()
	close(q.scanClose)
	q.scanClose = nil
	q.scanCloseMutex.Unlock()
	close(scanRes)

	bulkQuery.CleanUp()

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

	close(q.statChan)

	// Wait on the stat collector to finish (and print its results):
	q.statGroup.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err = fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	if q.reportTelemetry {
		fmt.Println("shutting down telemetry...")
		close(telemetryChanPoints)
		<-telemetryChanDone
		fmt.Println("done shutting down telemetry.")
	}

	if q.notificationServer != nil {
		fmt.Println("shutting down notification listener...")
		q.notificationServer.Shutdown(context.Background())
		fmt.Println("done shutting down notification listener.")
	}

	if q.notificationHostPort != "" {
		fmt.Printf("notify target %s...\n", q.notificationHostPort)
		q.notify(q.notificationHostPort)
	}

	if q.notificationGroup != "" && !q.sigtermReceived {
		siblings := strings.Split(q.notificationGroup, ",")
		for _, sibling := range siblings {
			fmt.Printf("notify sibling %s...\n", sibling)
			q.notify(sibling)
		}
	}

	if q.reportHost != "" {
		found := false
		for _, pair := range q.reportTags {
			if pair[0] == "use_case" {
				found = true
				break
			}
		}
		if q.useCase != "" && !found {
			q.reportTags = append(q.reportTags, [2]string{"use_case", q.useCase})
		}
		extraVals := make([]report.ExtraVal, 0, 1)
		q.reportTags = append(q.reportTags, [2]string{"batch_size", fmt.Sprintf("%d", q.batchSize)})
		q.reportTags = append(q.reportTags, [2]string{"wait_interval", q.waitInterval.String()})
		q.reportTags = append(q.reportTags, [2]string{"grad_workers_inc", fmt.Sprintf("%v", q.gradualWorkersIncrease)})
		q.reportTags = append(q.reportTags, [2]string{"increase_interval", q.increaseInterval.String()})
		q.reportTags = append(q.reportTags, [2]string{"benchmark_duration", q.testDuration.String()})
		q.reportTags = append(q.reportTags, [2]string{"response_time_limit", q.responseTimeLimit.String()})
		if responseTimeLimitReached {
			q.reportTags = append(q.reportTags, [2]string{"response_time_limit_reached", fmt.Sprintf("%v", responseTimeLimitReached)})
			extraVals = append(extraVals, report.ExtraVal{Name: "response_time_limit_workers", Value: int64(reponseTimeLimitWorkers)})
		}

		reportParams := &report.QueryReportParams{
			ReportParams: report.ReportParams{
				ReportDatabaseName: q.reportDatabase,
				ReportHost:         q.reportHost,
				ReportUser:         q.reportUser,
				ReportPassword:     q.reportPassword,
				Hostname:           q.reportHostname,
				Workers:            q.workers,
				ItemLimit:          int(q.limit),
			},
			BurnIn: int64(q.burnIn),
		}

		reportParams.ReportTags, extraVals = bulkQuery.UpdateReport(reportParams, q.reportTags, extraVals)

		if len(q.statMapping) > 2 {
			for query, stat := range q.statMapping {
				err = report.ReportQueryResult(reportParams, query, stat.Min, stat.Mean, stat.Max, stat.Count, wallTook, extraVals...)
				if err != nil {
					log.Fatal(err)
				}
			}
		} else {
			stat := q.statMapping[AllQueriesLabel]
			err = report.ReportQueryResult(reportParams, AllQueriesLabel, stat.Min, stat.Mean, stat.Max, stat.Count, wallTook, extraVals...)
			if err != nil {
				log.Fatal(err)
			}
		}

	}

	// (Optional) create a memory profile:
	if q.memProfile != "" {
		f, err := os.Create(q.memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}

}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func (q *QueryBenchmarker) processStats(telemetrySink chan *report.Point) {

	q.statMapping = StatsMap{
		AllQueriesLabel: &StatGroup{},
	}

	lastRefresh := time.Time{}
	i := uint64(0)
	for stat := range q.statChan {
		q.isBurnIn = i < q.burnIn
		if q.isBurnIn {
			i++
			q.statPool.Put(stat)
			continue
		} else if i == q.burnIn && q.burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", q.burnIn, q.workers)
			if err != nil {
				log.Fatal(err)
			}
		}

		if _, ok := q.statMapping[string(stat.Label)]; !ok {
			q.statMapping[string(stat.Label)] = &StatGroup{}
		}

		now := time.Now()

		if stat.IsActual {
			q.movingAverageStat.Push(now, stat.Value)
			q.statMapping[AllQueriesLabel].Push(stat.Value)
			q.statMapping[string(stat.Label)].Push(stat.Value)
			i++
		}

		q.statPool.Put(stat)

		if lastRefresh.Nanosecond() == 0 || now.Sub(lastRefresh).Seconds() >= 1.0 {
			q.movingAverageStat.UpdateAvg(now, q.workers)
			lastRefresh = now
			// Report telemetry, if applicable:
			if telemetrySink != nil {
				p := report.GetPointFromGlobalPool()
				p.Init("benchmarks_telemetry", now.UnixNano())
				for _, tagpair := range q.reportTags {
					p.AddTag(tagpair[0], tagpair[1])
				}
				p.AddTag("client_type", "query")
				p.AddFloat64Field("query_response_time_mean", q.statMapping[AllQueriesLabel].Mean)
				p.AddFloat64Field("query_response_time_moving_mean", q.movingAverageStat.Avg())
				p.AddIntField("query_workers", q.workers)
				p.AddInt64Field("queries", int64(i))
				telemetrySink <- p
			}
		}
		// print stats to stderr (if printInterval is greater than zero):
		if q.printInterval > 0 && i > 0 && i%q.printInterval == 0 && (int64(i) < q.limit || q.limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "%s: after %d queries with %d workers:\n", time.Now().String(), i-q.burnIn, q.workers)
			if err != nil {
				log.Fatal(err)
			}
			fprintStats(os.Stderr, q.statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}

	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-q.burnIn, q.workers)
	if err != nil {
		log.Fatal(err)
	}
	fprintStats(os.Stdout, q.statMapping)
	q.statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func fprintStats(w io.Writer, statGroups StatsMap) {
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

func (q *QueryBenchmarker) stopScan() {
	q.scanCloseMutex.Lock()
	if q.scanClose != nil {
		q.scanClose <- 1
	}
	q.scanCloseMutex.Unlock()
}

func (q *QueryBenchmarker) notify(target string) {
	client, err := rpc.DialHTTP("tcp", target)
	if err != nil {
		log.Printf("error dialing %s: %v\n", target, err)
	} else {
		var res int
		input := 0
		call := client.Go("NotifyReceiver.Notify", input, &res, nil)
		if call.Error != nil {
			log.Printf("error calling %s: %v\n", target, call.Error)
		}
		client.Close()
	}
}

func (q *QueryBenchmarker) notifyHandler(arg int) (int, error) {
	var e error
	if arg == 0 {
		log.Println("Received external terminate request")
		if !q.sigtermReceived {
			q.sigtermReceived = true
			q.stopScan()
		} else {
			log.Println("External terminate request already received")
		}
	} else {
		e = fmt.Errorf("unknown notification code: %d", arg)
	}
	return 0, e
}
