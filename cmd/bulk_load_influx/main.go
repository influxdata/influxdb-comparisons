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
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
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
const ValuesPerMeasurement = 9.636 // dashboard use-case, original value was: 11.2222

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
	timeLimit              time.Duration
	progressInterval       time.Duration
	doLoad                 bool
	doDBCreate             bool
	useGzip                bool
	doAbortOnExist         bool
	memprofile             bool
	cpuProfileFile         string
	consistency            string
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
	notificationListenPort int
)

// Global vars
var (
	bufPool               sync.Pool
	batchChan             chan *bytes.Buffer
	inputDone             chan struct{}
	workersGroup          sync.WaitGroup
	backingOffChans       []chan bool
	backingOffDones       []chan struct{}
	telemetryChanPoints   chan *report.Point
	telemetryChanDone     chan struct{}
	syncChanDone          chan int
	telemetrySrcAddr      string
	telemetryTags         [][2]string
	progressIntervalItems uint64
	reportTags            [][2]string
	reportHostname        string
	ingestionRateGran     float32
	endedPrematurely      bool
	prematureEndReason    string
)

var consistencyChoices = map[string]struct{}{
	"any":    struct{}{},
	"one":    struct{}{},
	"quorum": struct{}{},
	"all":    struct{}{},
}

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database name.")
	flag.IntVar(&replicationFactor, "replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flag.StringVar(&consistency, "consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size (1 line of input = 1 item).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.IntVar(&ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in values/s (-1 = no limit).")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.DurationVar(&timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.DurationVar(&progressInterval, "progress-interval", -1, "Duration between printing progress messages.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDBCreate, "do-db-create", true, "Whether to create the database.")
	flag.BoolVar(&doAbortOnExist, "do-abort-on-exist", true, "Whether to abort if the destination database already exists.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.StringVar(&telemetryBasicAuth, "telemetry-basic-auth", "", "basic auth (username:password) for telemetry.")
	flag.StringVar(&telemetryTagsCSV, "telemetry-tags", "", "Tag(s) for telemetry. Format: key0:val0,key1:val1,...")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 10, "Telemetry batch size (lines).")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")
	flag.IntVar(&notificationListenPort, "notification-port", -1, "Listen port for remote notification messages. Used to remotely finish benchmark. -1 to disable feature")
	flag.StringVar(&cpuProfileFile, "cpu-profile", "", "Write cpu profile to `file`")

	flag.Parse()

	if _, ok := consistencyChoices[consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

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

	if ingestRateLimit > 0 {
		ingestionRateGran = (float32(ingestRateLimit) / float32(workers)) / (float32(1000) / float32(RateControlGranularity))
		log.Printf("Using worker ingestion rate %v values/%v ms", ingestionRateGran, RateControlGranularity)
		recommendedBatchSize := int((ingestionRateGran / ValuesPerMeasurement) * 0.20)
		log.Printf("Calculated batch size hint: %v (allowed min: %v max: %v)", recommendedBatchSize, RateControlMinBatchSize, batchSize)
		if recommendedBatchSize < RateControlMinBatchSize {
			recommendedBatchSize = RateControlMinBatchSize
		} else if recommendedBatchSize > batchSize {
			recommendedBatchSize = batchSize
		}
		if recommendedBatchSize != batchSize {
			log.Printf("Adjusting batchSize from %v to %v (%v values in 1 batch)", batchSize, recommendedBatchSize, float32(recommendedBatchSize)*ValuesPerMeasurement)
			batchSize = recommendedBatchSize
		}
	} else {
		log.Printf("Ingestion rate control is off")
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

	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})
	syncChanDone = make(chan int)

	backingOffChans = make([]chan bool, workers)
	backingOffDones = make([]chan struct{}, workers)

	if telemetryHost != "" {
		telemetryCollector := report.NewCollector(telemetryHost, "telegraf", telemetryBasicAuth)
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

	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
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
		go processBatches(NewHTTPWriter(cfg, consistency), backingOffChans[i], backingOffDones[i], telemetryChanPoints, fmt.Sprintf("%d", i))
		go processBackoffMessages(i, backingOffChans[i], backingOffDones[i])
	}

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

	workersGroup.Wait()

	for i := range backingOffChans {
		close(backingOffChans[i])
		<-backingOffDones[i]
	}

	end := time.Now()
	took := end.Sub(start)

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	if telemetryHost != "" {
		close(telemetryChanPoints)
		<-telemetryChanDone
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		reportTags = append(reportTags, [2]string{"replication_factor", strconv.Itoa(int(replicationFactor))})
		reportTags = append(reportTags, [2]string{"back_off", strconv.Itoa(int(backoff.Seconds()))})
		reportTags = append(reportTags, [2]string{"consistency", consistency})
		if endedPrematurely {
			reportTags = append(reportTags, [2]string{"premature_end_reason", report.Escape(prematureEndReason)})
		}
		if timeLimit.Seconds() > 0 {
			reportTags = append(reportTags, [2]string{"time_limit", timeLimit.String()})
		}
		if ingestRateLimit > 0 {
			reportTags = append(reportTags, [2]string{"ingest_rate_limit", strconv.Itoa(ingestRateLimit)})
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
		err = report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took)

		if err != nil {
			log.Fatal(err)
		}
	}

}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func scan(itemsPerBatch int, doneCh chan int) (int64, int64, int64) {
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
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0

			if timeLimit > 0 && time.Now().After(deadline) {
				endedPrematurely = true
				prematureEndReason = "Timeout elapsed"
				break outer
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
		batchChan <- buf
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

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter, backoffSrc chan bool, backoffDst chan struct{}, telemetrySink chan *report.Point, telemetryWorkerLabel string) {
	var batchesSeen int64

	// Ingestion rate control vars
	var gvCount float32
	var gvStart time.Time
	var ingestionRateDebt int64

	if ingestRateLimit > 0 {
		gvStart = time.Now()
	}

	for batch := range batchChan {
		batchesSeen++

		var bodySize int
		ts := time.Now().UnixNano()

		// Write the batch: try until backoff is not needed.
		if doLoad {
			var err error
			for {
				if useGzip {
					compressedBatch := bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.Bytes())
					bodySize = len(compressedBatch.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					// Return the compressed batch buffer to the pool.
					compressedBatch.Reset()
					bufPool.Put(compressedBatch)
				} else {
					bodySize = len(batch.Bytes())
					_, err = w.WriteLineProtocol(batch.Bytes(), false)
				}

				if err == BackoffError {
					backoffSrc <- true
					time.Sleep(backoff)
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				log.Fatalf("Error writing: %s\n", err.Error())
			}
		}

		if ingestRateLimit > 0 {
			gvCount += float32(batchSize) * ValuesPerMeasurement // TODO last batch may not be full batchSize
			if gvCount >= ingestionRateGran {
				now := time.Now()
				remaining := now.Sub(gvStart)
				remainingMs := RateControlGranularity - (remaining.Nanoseconds() / 1e6) + ingestionRateDebt
				ingestionRateDebt = 0
				if remainingMs > 0 {
					time.Sleep(time.Duration(remainingMs) * time.Millisecond) // TODO discount 5 ms for syscalls (sleep & wakeup) overhead?
					gvStart = time.Now()
					realDelay := gvStart.Sub(now).Nanoseconds() / 1e6
					if realDelay != remainingMs {
						ingestionRateDebt = -(realDelay - remainingMs) // TODO how about spurios wakeups?
					}
				} else {
					gvStart = now
					ingestionRateDebt = remainingMs
				}
				if ingestionRateDebt != 0 {
					ingestionRateDebt = int64(float64(ingestionRateDebt) * float64(1.05))
					if ingestionRateDebt < -RateControlGranularity { // trim to monitored period
						ingestionRateDebt = -RateControlGranularity
					}
				}
				gvCount -= ingestionRateGran
			}
		}

		// lagMillis intentionally includes backoff time,
		// and incidentally includes compression time:
		lagMillis := float64(time.Now().UnixNano()-ts) / 1e6

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := report.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			for _, tagpair := range telemetryTags {
				p.AddTag(tagpair[0], tagpair[1])
			}
			p.AddTag("src_addr", telemetrySrcAddr)
			p.AddTag("dst_addr", w.c.Host)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddFloat64Field("rtt_ms_total", lagMillis)
			p.AddBoolField("gzip", useGzip)
			p.AddInt64Field("body_bytes", int64(bodySize))
			telemetrySink <- p
		}
	}
	workersGroup.Done()
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
}

func createDb(daemon_url, dbname string, replicationFactor int) error {
	u, err := url.Parse(daemon_url)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
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
