// bulk_load_influx loads an InfluxDB daemon with data from stdin.
// bulk_load_influx loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
	"strconv"
)

// TODO VH: This should be calculated from available simulation data
const ValuesPerMeasurement = 9.63636 // dashboard use-case, original value was: 11.2222

// TODO AP: Maybe useless
const RateControlGranularity = 1000 // 1000 ms = 1s
const RateControlMinBatchSize = 100

// Program option vars:
var (
	csvDaemonUrls          string
	daemonUrls             []string
	dbName                 string
	replicationFactor      int
	workers                int
	itemLimit              int64
	batchSize              int
	ingestRateLimit        int
	backoff                time.Duration
	backoffTimeOut         time.Duration
	timeLimit              time.Duration
	progressInterval       time.Duration
	doLoad                 bool
	doDBCreate             bool
	useGzip                bool
	doAbortOnExist         bool
	memprofile             bool
	cpuProfileFile         string
	consistency            string
	telemetryStderr        bool
	telemetryBatchSize     uint64
	reportDatabase         string
	reportHost             string
	reportUser             string
	reportPassword         string
	reportTagsCSV          string
	reportTelemetry        bool
	notificationListenPort int
	clientIndex            int
	printInterval          uint64
	trendSamples           int
	movingAverageInterval  time.Duration
)

// Global vars
var (
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	workersGroup          sync.WaitGroup
	backingOffChans       []chan bool
	backingOffDones       []chan struct{}
	telemetryChanPoints   chan *report.Point
	telemetryChanDone     chan struct{}
	syncChanDone          chan int
	progressIntervalItems uint64
	reportTags            [][2]string
	reportHostname        string
	ingestionRateGran     float64
	endedPrematurely      bool
	prematureEndReason    string
	maxBatchSize          int
	speedUpRequest        int32
	statMapping           statsMap
	statPool              sync.Pool
	statChan              chan *Stat
	statGroup             sync.WaitGroup
	movingAverageStat     *TimedStatGroup
	scanFinished          bool
)

var consistencyChoices = map[string]struct{}{
	"any":    {},
	"one":    {},
	"quorum": {},
	"all":    {},
}

type statsMap map[string]*StatGroup

type batch struct {
	Buffer *bytes.Buffer
	Items  int
}

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database name.")
	flag.IntVar(&replicationFactor, "replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flag.StringVar(&consistency, "consistency", "one", "Write consistency. Must be one of: any, one, quorum, all.")
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (1 line of input = 1 item).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in values/s (-1 = no limit).")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.DurationVar(&backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	flag.DurationVar(&timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.DurationVar(&progressInterval, "progress-interval", -1, "Duration between printing progress messages.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDBCreate, "do-db-create", true, "Whether to create the database.")
	flag.BoolVar(&doAbortOnExist, "do-abort-on-exist", true, "Whether to abort if the destination database already exists.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 1, "Telemetry batch size (lines).")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")
	flag.BoolVar(&reportTelemetry, "report-telemetry", false, "Turn on/off reporting telemetry")
	flag.IntVar(&notificationListenPort, "notification-port", -1, "Listen port for remote notification messages. Used to remotely finish benchmark. -1 to disable feature")
	flag.StringVar(&cpuProfileFile, "cpu-profile", "", "Write cpu profile to `file`")
	flag.IntVar(&clientIndex, "client-index", 0, "Index of a client host running this tool. Used to distribute load")
	flag.Uint64Var(&printInterval, "print-interval", 1000, "Print timing stats to stderr after this many batches (0 to disable)")
	flag.DurationVar(&movingAverageInterval, "moving-average-interval", time.Second*30, "Interval of measuring mean write rate on which moving average is calculated.")

	flag.Parse()

	if _, ok := consistencyChoices[consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

	if workers < 1 {
		log.Fatalf("invalid number of workers: %d\n", workers)
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

	if ingestRateLimit > 0 {
		ingestionRateGran = (float64(ingestRateLimit) / float64(workers)) / (float64(1000) / float64(RateControlGranularity))
		log.Printf("Using worker ingestion rate %v values/%v ms", ingestionRateGran, RateControlGranularity)
		recommendedBatchSize := int((ingestionRateGran / ValuesPerMeasurement) * 0.20)
		log.Printf("Calculated batch size hint: %v (allowed min: %v max: %v)", recommendedBatchSize, RateControlMinBatchSize, batchSize)
		if recommendedBatchSize < RateControlMinBatchSize {
			recommendedBatchSize = RateControlMinBatchSize
		} else if recommendedBatchSize > batchSize {
			recommendedBatchSize = batchSize
		}
		maxBatchSize = batchSize
		if recommendedBatchSize != batchSize {
			log.Printf("Adjusting batchSize from %v to %v (%v values in 1 batch)", batchSize, recommendedBatchSize, float32(recommendedBatchSize)*ValuesPerMeasurement)
			batchSize = recommendedBatchSize
		}
	} else {
		log.Printf("Ingestion rate control is off")
	}

	if trendSamples <= 0 {
		trendSamples = int(movingAverageInterval.Seconds())
	}

	if timeLimit > 0 && backoffTimeOut > timeLimit {
		backoffTimeOut = timeLimit
	}
}

func notifyHandler(arg int) (int, error) {
	var e error
	if arg == 0 {
		fmt.Println("Received external finish request")
		endedPrematurely = true
		prematureEndReason = "External notification"
		syncChanDone <- 1
	} else {
		e = fmt.Errorf("unknown notification code: %d", arg)
	}
	return 0, e
}

func printInfo() {
	fmt.Printf("SysInfo:\n")
	fmt.Printf("  Current GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))
	fmt.Printf("  Num CPUs: %d\n", runtime.NumCPU())
}
func main() {
	exitCode := 0
	printInfo()
	if memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}
	if cpuProfileFile != "" {
		f, err := os.Create(cpuProfileFile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	// check that there are no pre-existing databases
	// this also test db connection
	existingDatabases, err := listDatabases(daemonUrls[0])
	if err != nil {
		log.Fatal(err)
	}
	if doLoad && doDBCreate {

		if len(existingDatabases) > 0 {
			if doAbortOnExist {
				log.Fatalf("There are databases already in the data store. If you know what you are doing, run the command:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", existingDatabases[0])
			} else {
				log.Printf("Info: there are databases already in the data store.")
			}
		}

		if len(existingDatabases) == 0 {
			err = createDb(daemonUrls[0], dbName, replicationFactor)
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{}
		},
	}

	movingAverageStat = NewTimedStatGroup(movingAverageInterval, trendSamples)

	batchChan = make(chan batch, workers)
	inputDone = make(chan struct{})
	syncChanDone = make(chan int)

	backingOffChans = make([]chan bool, workers)
	backingOffDones = make([]chan struct{}, workers)
	backingOffSecs := make([]float64, workers)

	if reportHost != "" && reportTelemetry {
		telemetryCollector := report.NewCollector(reportHost, reportDatabase, reportUser, reportPassword)
		err = telemetryCollector.CreateDatabase()
		if err != nil {
			log.Fatalf("Error creating telemetry db: %v\n", err)
		}
		telemetryChanPoints, telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, telemetryBatchSize, telemetryStderr, 0)
	}

	if notificationListenPort > 0 {
		notif := new(bulk_load.NotifyReceiver)
		rpc.Register(notif)
		rpc.HandleHTTP()
		bulk_load.RegisterHandler(notifyHandler)
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", notificationListenPort))
		if e != nil {
			log.Fatal("listen error:", e)
		}
		log.Println("Listening for incoming notification")
		go http.Serve(l, nil)
	}

	statChan = make(chan *Stat, workers)
	statGroup.Add(1)
	go processStats(telemetryChanPoints)
	var once sync.Once

	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[(i+clientIndex)%len(daemonUrls)]
		backingOffChans[i] = make(chan bool, 100)
		backingOffDones[i] = make(chan struct{})
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, daemonUrl),
			Host:           daemonUrl,
			Database:       dbName,
			BackingOffChan: backingOffChans[i],
			BackingOffDone: backingOffDones[i],
		}
		go func(w int) {
			err := processBatches(NewHTTPWriter(cfg, consistency), backingOffChans[w], telemetryChanPoints, fmt.Sprintf("%d", w))
			if err != nil {
				log.Printf("Worker %d: error: %s\n", w, err.Error())
				once.Do(func() {
					log.Printf("Worker %d:  preparing exit\n", w)
					endedPrematurely = true
					prematureEndReason = "Worker error"
					if !scanFinished {
						go func() {
							for range batchChan {
								//read out remaining batches
							}
						}()
						log.Printf("Worker %d:  Finishing scan\n", w)
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			backingOffSecs[w] = processBackoffMessages(w, backingOffChans[w], backingOffDones[w])
		}(i)
	}
	fmt.Printf("Started load with %d workers\n", workers)

	if progressInterval >= 0 {
		go func() {
			start := time.Now()
			for end := range time.NewTicker(progressInterval).C {
				n := atomic.SwapUint64(&progressIntervalItems, 0)

				//absoluteMillis := end.Add(-progressInterval).UnixNano() / 1e6
				absoluteMillis := start.UTC().UnixNano() / 1e6
				fmt.Printf("[interval_progress_items] %dms, %d\n", absoluteMillis, n)
				start = end
			}
		}()
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scan(batchSize, syncChanDone)

	<-inputDone
	close(batchChan)
	close(syncChanDone)

	log.Println("Waiting for workers")
	workersGroup.Wait()

	close(statChan)
	statGroup.Wait()
	log.Println("Closing backoff handlers")
	for i := range backingOffChans {
		close(backingOffChans[i])
		<-backingOffDones[i]
	}

	end := time.Now()
	took := end.Sub(start)

	totalBackOffSecs := float64(0)
	for i := 0; i < workers; i++ {
		totalBackOffSecs += backingOffSecs[i]
	}

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	if reportHost != "" && reportTelemetry {
		close(telemetryChanPoints)
		<-telemetryChanDone
	}
	if endedPrematurely {
		fmt.Printf("load finished prematurely: %s\n", prematureEndReason)
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		reportTags = append(reportTags, [2]string{"back_off", strconv.Itoa(int(backoff.Seconds()))})
		reportTags = append(reportTags, [2]string{"consistency", consistency})
		if endedPrematurely {
			reportTags = append(reportTags, [2]string{"premature_end_reason", report.Escape(prematureEndReason)})
		}
		if timeLimit.Seconds() > 0 {
			reportTags = append(reportTags, [2]string{"time_limit", timeLimit.String()})
		}
		extraVals := make([]report.ExtraVal, 0, 1)
		if ingestRateLimit > 0 {
			extraVals = append(extraVals, report.ExtraVal{Name: "ingest_rate_limit_values", Value: ingestRateLimit})
		}
		if totalBackOffSecs > 0 {
			extraVals = append(extraVals, report.ExtraVal{Name: "total_backoff_secs", Value: totalBackOffSecs})
		}

		reportParams := &report.LoadReportParams{
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
				ItemLimit:          int(itemLimit),
			},
			IsGzip:    useGzip,
			BatchSize: batchSize,
		}
		err = report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took, extraVals...)

		if err != nil {
			log.Fatal(err)
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}

}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func scan(itemsPerBatch int, doneCh chan int) (int64, int64, int64) {
	scanFinished = false
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	newline := []byte("\n")
	var deadline time.Time
	if timeLimit > 0 {
		deadline = time.Now().Add(timeLimit)
	}

	var batchItemCount uint64

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))
outer:
	for scanner.Scan() {
		if itemsRead == itemLimit {
			break
		}

		line := scanner.Text()
		if strings.HasPrefix(line, common.DatasetSizeMarker) {
			parts := common.DatasetSizeMarkerRE.FindAllStringSubmatch(line, -1)
			if parts == nil || len(parts[0]) != 3 {
				log.Fatalf("Incorrent number of matched groups: %#v", parts)
			}
			if i, err := strconv.Atoi(parts[0][1]); err == nil {
				totalPoints = int64(i)
			} else {
				log.Fatal(err)
			}
			if i, err := strconv.Atoi(parts[0][2]); err == nil {
				totalValues = int64(i)
			} else {
				log.Fatal(err)
			}
			continue
		}
		itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= itemsPerBatch {
			atomic.AddUint64(&progressIntervalItems, batchItemCount)
			batchItemCount = 0

			bytesRead += int64(buf.Len())
			batchChan <- batch{buf, n}
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0

			if timeLimit > 0 && time.Now().After(deadline) {
				endedPrematurely = true
				prematureEndReason = "Timeout elapsed"
				break outer
			}

			if ingestRateLimit > 0 {
				if itemsPerBatch < maxBatchSize {
					hint := atomic.LoadInt32(&speedUpRequest)
					if hint > int32(workers*2) { // we should wait for more requests (and this is just a magic number)
						atomic.StoreInt32(&speedUpRequest, 0)
						itemsPerBatch += int(float32(maxBatchSize) * 0.10)
						if itemsPerBatch > maxBatchSize {
							itemsPerBatch = maxBatchSize
						}
						log.Printf("Increased batch size to %d\n", itemsPerBatch)
					}
				}
			}
		}
		select {
		case <-doneCh:
			break outer
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- batch{buf, n}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	if itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !endedPrematurely {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", itemsRead, totalPoints)
		} else {
			totalValues = int64(float64(itemsRead) * ValuesPerMeasurement) // needed for statistics summary
		}
	}
	scanFinished = true
	log.Println("Scan finished")
	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter, backoffSrc chan bool, telemetrySink chan *report.Point, telemetryWorkerLabel string) error {
	var batchesSeen int64

	// Ingestion rate control vars
	var gvCount float64
	var gvStart time.Time

	defer workersGroup.Done()

	for batch := range batchChan {
		batchesSeen++

		//var bodySize int
		ts := time.Now().UnixNano()

		if ingestRateLimit > 0 && gvStart.Nanosecond() == 0 {
			gvStart = time.Now()
		}

		// Write the batch: try until backoff is not needed.
		if doLoad {
			var err error
			sleepTime := backoff
			timeStart := time.Now()
			for {
				if useGzip {
					compressedBatch := bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.Buffer.Bytes())
					//bodySize = len(compressedBatch.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					// Return the compressed batch buffer to the pool.
					compressedBatch.Reset()
					bufPool.Put(compressedBatch)
				} else {
					//bodySize = len(batch.Bytes())
					_, err = w.WriteLineProtocol(batch.Buffer.Bytes(), false)
				}

				if err == BackoffError {
					backoffSrc <- true
					// Report telemetry, if applicable:
					if telemetrySink != nil {
						p := report.GetPointFromGlobalPool()
						p.Init("benchmarks_telemetry", ts)
						for _, tagpair := range reportTags {
							p.AddTag(tagpair[0], tagpair[1])
						}
						p.AddTag("client_type", "load")
						p.AddTag("worker", telemetryWorkerLabel)
						p.AddBoolField("backoff", true)
						telemetrySink <- p
					}
					time.Sleep(sleepTime)
					sleepTime += backoff        // sleep longer if backpressure comes again
					if sleepTime > 10*backoff { // but not longer than 10x default backoff time
						log.Printf("[worker %s] sleeping on backoff response way too long (10x %v)", telemetryWorkerLabel, backoff)
						sleepTime = 10 * backoff
					}
					checkTime := time.Now()
					if timeStart.Add(backoffTimeOut).Before(checkTime) {
						log.Printf("[worker %s] Spent too much time in backoff: %ds\n", telemetryWorkerLabel, int64(checkTime.Sub(timeStart).Seconds()))
						break
					}
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				return fmt.Errorf("Error writing: %s\n", err.Error())
			}
		}

		// lagMillis intentionally includes backoff time,
		// and incidentally includes compression time:
		lagMillis := float64(time.Now().UnixNano()-ts) / 1e6

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		bufPool.Put(batch.Buffer)

		// Normally report after each batch
		reportStat := true
		valuesWritten := float64(batch.Items) * ValuesPerMeasurement

		// Apply ingest rate control if set
		if ingestRateLimit > 0 {
			gvCount += valuesWritten
			if gvCount >= ingestionRateGran {
				now := time.Now()
				elapsed := now.Sub(gvStart)
				overdelay := (gvCount - ingestionRateGran) / (ingestionRateGran / float64(RateControlGranularity))
				remainingMs := RateControlGranularity - (elapsed.Nanoseconds() / 1e6) + int64(overdelay)
				valuesWritten = gvCount
				lagMillis = float64(elapsed.Nanoseconds() / 1e6)
				if remainingMs > 0 {
					time.Sleep(time.Duration(remainingMs) * time.Millisecond)
					gvStart = time.Now()
					realDelay := gvStart.Sub(now).Nanoseconds() / 1e6 // 'now' was before sleep
					lagMillis += float64(realDelay)
				} else {
					gvStart = now
					atomic.AddInt32(&speedUpRequest, 1)
				}
				gvCount = 0
			} else {
				reportStat = false
			}
		}

		// Report sent batch statistic
		if reportStat {
			stat := statPool.Get().(*Stat)
			stat.Label = []byte(telemetryWorkerLabel)
			stat.Value = valuesWritten
			statChan <- stat
		}
	}

	return nil
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) float64 {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			log.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
	return totalBackoffSecs
}

func createDb(daemonUrl, dbname string, replicationFactor int) error {
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", consistency)
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbname, replicationFactor))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}
	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func listDatabases(daemonUrl string) ([]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, nestedName := range listing.Results[0].Series[0].Values {
		name := nestedName[0]
		// the _internal database is skipped:
		if name == "_internal" {
			continue
		}
		ret = append(ret, name)
	}
	return ret, nil
}

var firstStat time.Time

func processStats(telemetrySink chan *report.Point) {

	statMapping = statsMap{
		"*": &StatGroup{},
	}

	lastRefresh := time.Time{}
	i := uint64(0)
	for stat := range statChan {
		now := time.Now()
		if lastRefresh.Nanosecond() == 0 {
			log.Print("First statistic report received")
			lastRefresh = now
			firstStat = now
		}

		movingAverageStat.Push(now, stat.Value)
		statMapping["*"].Push(stat.Value)

		statPool.Put(stat)

		i++

		dt := now.Sub(lastRefresh).Seconds()
		if dt >= 1 {
			movingAverageStat.UpdateAvg(now, workers)
			lastRefresh = now
			// Report telemetry, if applicable:
			if telemetrySink != nil {
				p := report.GetPointFromGlobalPool()
				p.Init("benchmarks_telemetry", now.UnixNano())
				for _, tagpair := range reportTags {
					p.AddTag(tagpair[0], tagpair[1])
				}
				p.AddTag("client_type", "load")
				p.AddFloat64Field("ingest_rate_mean", statMapping["*"].Sum/now.Sub(firstStat).Seconds()) /*statMapping["*"].Mean*/
				p.AddFloat64Field("ingest_rate_moving_mean", movingAverageStat.Rate())
				p.AddIntField("load_workers", workers)
				telemetrySink <- p
			}
			log.Printf("mean updated after %f", dt)
		}

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 {
			_, err := fmt.Fprintf(os.Stderr, "%s: after %d batches:\n", time.Now().String(), i)
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
	_, err := fmt.Printf("run complete after %d batches:\n", i)
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
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}
		_, err := fmt.Fprintf(w, "%s : min: %8.2f/s, mean: %8.2f/s, moving mean: %8.2f/s, moving median: %8.2f/s, max: %7.2f/s, count: %8d, sum: %f \n", paddedKey, math.NaN(), v.Sum/time.Now().Sub(firstStat).Seconds(), movingAverageStat.Rate(), math.NaN(), math.NaN(), v.Count, v.Sum)
		if err != nil {
			log.Fatal(err)
		}
	}
}
