package bulk_load

import (
	"flag"
	"fmt"
	"github.com/pkg/profile"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/report"
)

// TODO: distinguish per use case
const ValuesPerMeasurement = 9.63636 // dashboard use-case, original value was: 11.2222

type BulkLoad interface {
	Init()
	Validate()
	CreateDb()
	PrepareWorkers()
	GetBatchProcessor() BatchProcessor
	GetScanner() Scanner
	SyncEnd()
	CleanUp()
	UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal)
}

type LoadRunner struct {
	DbName                 string
	Workers                int
	ItemLimit              int64
	BatchSize              int
	TimeLimit              time.Duration
	progressInterval       time.Duration
	DoLoad                 bool
	DoDBCreate             bool
	DoAbortOnExist         bool
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
	printInterval          uint64
	trendSamples           int
	movingAverageInterval  time.Duration

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
	StatPool              sync.Pool
	StatChan              chan *Stat
	statGroup             sync.WaitGroup
	movingAverageStat     *TimedStatGroup
	scanFinished          bool
}

var Runner = &LoadRunner{}

type statsMap map[string]*StatGroup

func (r *LoadRunner) notifyHandler(arg int) (int, error) {
	var e error
	if arg == 0 {
		fmt.Println("Received external finish request")
		r.SetPrematureEnd("External notification")
		r.syncChanDone <- 1
	} else {
		e = fmt.Errorf("unknown notification code: %d", arg)
	}
	return 0, e
}

func (r *LoadRunner) Init(defaultBatchSize int) {
	flag.StringVar(&r.DbName, "db", "benchmark_db", "Database name.")
	flag.IntVar(&r.BatchSize, "batch-size", defaultBatchSize, "Batch size (1 line of input = 1 item).")
	flag.IntVar(&r.Workers, "Workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&r.ItemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")
	flag.Uint64Var(&r.printInterval, "print-interval", 0, "Print timing stats to stderr after this many batches (0 to disable)")
	flag.DurationVar(&r.movingAverageInterval, "moving-average-interval", time.Second*30, "Interval of measuring mean write rate on which moving average is calculated.")
	flag.DurationVar(&r.TimeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.DurationVar(&r.progressInterval, "progress-interval", -1, "Duration between printing progress messages.")
	flag.StringVar(&r.cpuProfileFile, "cpu-profile", "", "Write cpu profile to `file`")
	flag.BoolVar(&r.DoLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&r.DoDBCreate, "do-db-create", true, "Whether to create the database.")
	flag.BoolVar(&r.DoAbortOnExist, "do-abort-on-exist", true, "Whether to abort if the destination database already exists.")
	flag.BoolVar(&r.memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.BoolVar(&r.telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&r.telemetryBatchSize, "telemetry-batch-size", 1, "Telemetry batch size (lines).")
	flag.StringVar(&r.reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&r.reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&r.reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&r.reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&r.reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")
	flag.BoolVar(&r.reportTelemetry, "report-telemetry", false, "Turn on/off reporting telemetry")
	flag.IntVar(&r.notificationListenPort, "notification-port", -1, "Listen port for remote notification messages. Used to remotely finish benchmark. -1 to disable feature")
}

func (r *LoadRunner) SetPrematureEnd(reason string) {
	r.endedPrematurely = true
	r.prematureEndReason = reason
}

func (r *LoadRunner) HasEndedPrematurely() bool {
	return r.endedPrematurely
}

func (r *LoadRunner) Validate() {

	if r.trendSamples <= 0 {
		r.trendSamples = int(r.movingAverageInterval.Seconds())
	}

	if r.Workers < 1 {
		log.Fatalf("invalid number of Workers: %d\n", r.Workers)
	}

	if r.reportHost != "" {
		fmt.Printf("results report destination: %v\n", r.reportHost)
		fmt.Printf("results report database: %v\n", r.reportDatabase)

		var err error
		r.reportHostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("hostname for results report: %v\n", r.reportHostname)

		if r.reportTagsCSV != "" {
			pairs := strings.Split(r.reportTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				r.reportTags = append(r.reportTags, tagpair)
			}
		}
		fmt.Printf("results report tags: %v\n", r.reportTags)
	}

}

func printInfo() {
	fmt.Printf("SysInfo:\n")
	fmt.Printf("  Current GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))
	fmt.Printf("  Num CPUs: %d\n", runtime.NumCPU())
}

func (r *LoadRunner) Run(load BulkLoad) int {
	exitCode := 0

	printInfo()
	if r.memprofile {
		p := profile.Start(profile.MemProfile)
		defer p.Stop()
	}
	if r.cpuProfileFile != "" {
		f, err := os.Create(r.cpuProfileFile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if r.DoLoad && r.DoDBCreate {
		load.CreateDb()
	}

	r.StatPool = sync.Pool{
		New: func() interface{} {
			return &Stat{}
		},
	}
	r.syncChanDone = make(chan int)

	r.movingAverageStat = NewTimedStatGroup(r.movingAverageInterval, r.trendSamples)

	if r.reportHost != "" && r.reportTelemetry {
		telemetryCollector := report.NewCollector(r.reportHost, r.reportDatabase, r.reportUser, r.reportPassword)
		err := telemetryCollector.CreateDatabase()
		if err != nil {
			log.Fatalf("Error creating telemetry db: %v\n", err)
		}
		r.telemetryChanPoints, r.telemetryChanDone = report.TelemetryRunAsync(telemetryCollector, r.telemetryBatchSize, r.telemetryStderr, 0)
	}

	if r.notificationListenPort > 0 {
		notif := new(NotifyReceiver)
		rpc.Register(notif)
		rpc.HandleHTTP()
		RegisterHandler(r.notifyHandler)
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", r.notificationListenPort))
		if e != nil {
			log.Fatal("listen error:", e)
		}
		log.Println("Listening for incoming notification")
		go http.Serve(l, nil)
	}

	r.StatChan = make(chan *Stat, r.Workers)
	r.statGroup.Add(1)
	go r.processStats(r.telemetryChanPoints)

	var once sync.Once
	var workersGroup sync.WaitGroup

	load.PrepareWorkers()

	batchProcessor := load.GetBatchProcessor()
	scanner := load.GetScanner()
	for i := 0; i < r.Workers; i++ {
		batchProcessor.PrepareProcess(i)
		workersGroup.Add(1)
		go func(w int) {
			err := batchProcessor.RunProcess(w, &workersGroup, r.telemetryChanPoints, r.reportTags)
			if err != nil {
				fmt.Println(err.Error())
				once.Do(func() {
					r.endedPrematurely = true
					r.prematureEndReason = "Worker error"
					if !scanner.IsScanFinished() {
						go func() {
							batchProcessor.EmptyBatchChanel()
						}()
						r.syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
		go func(w int) {
			batchProcessor.AfterRunProcess(w)
		}(i)
	}
	fmt.Printf("Started load with %d workers\n", r.Workers)

	if r.progressInterval >= 0 {
		go func() {
			start := time.Now()
			for end := range time.NewTicker(r.progressInterval).C {
				n := atomic.SwapUint64(&r.progressIntervalItems, 0)

				//absoluteMillis := end.Add(-progressInterval).UnixNano() / 1e6
				absoluteMillis := start.UTC().UnixNano() / 1e6
				fmt.Printf("[interval_progress_items] %dms, %d\n", absoluteMillis, n)
				start = end
			}
		}()
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scanner.RunScanner(r.syncChanDone)

	load.SyncEnd()
	close(r.syncChanDone)

	workersGroup.Wait()

	close(r.StatChan)
	r.statGroup.Wait()

	load.CleanUp()

	end := time.Now()
	took := end.Sub(start)

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	if r.reportHost != "" && r.reportTelemetry {
		close(r.telemetryChanPoints)
		<-r.telemetryChanDone
	}
	if r.endedPrematurely {
		fmt.Printf("load finished prematurely: %s\n", r.prematureEndReason)
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), r.Workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if r.reportHost != "" {
		//append db specific tags to custom tags
		if r.endedPrematurely {
			r.reportTags = append(r.reportTags, [2]string{"premature_end_reason", report.Escape(r.prematureEndReason)})
		}
		if r.TimeLimit.Seconds() > 0 {
			r.reportTags = append(r.reportTags, [2]string{"time_limit", r.TimeLimit.String()})
		}

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				ReportDatabaseName: r.reportDatabase,
				ReportHost:         r.reportHost,
				ReportUser:         r.reportUser,
				ReportPassword:     r.reportPassword,
				Hostname:           r.reportHostname,
				Workers:            r.Workers,
				ItemLimit:          int(r.ItemLimit),
			},
			IsGzip:    false,
			BatchSize: r.BatchSize,
		}
		customTags, extraVals := load.UpdateReport(reportParams)
		if customTags != nil {
			reportParams.ReportTags = append(r.reportTags, customTags...)
		}
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took, extraVals...)

		if err != nil {
			log.Fatal(err)
		}
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}

	return exitCode
}

var firstStat time.Time

func (r *LoadRunner) processStats(telemetrySink chan *report.Point) {

	r.statMapping = statsMap{
		"*": &StatGroup{},
	}

	lastRefresh := time.Time{}
	i := uint64(0)
	for stat := range r.StatChan {
		now := time.Now()
		if lastRefresh.Nanosecond() == 0 {
			log.Print("First statistic report received")
			lastRefresh = now
			firstStat = now
		}

		r.movingAverageStat.Push(now, stat.Value)
		r.statMapping["*"].Push(stat.Value)

		r.StatPool.Put(stat)

		i++

		if now.Sub(lastRefresh).Seconds() >= 1 {
			r.movingAverageStat.UpdateAvg(now, r.Workers)
			lastRefresh = now
			// Report telemetry, if applicable:
			if telemetrySink != nil {
				p := report.GetPointFromGlobalPool()
				p.Init("benchmarks_telemetry", now.UnixNano())
				for _, tagpair := range r.reportTags {
					p.AddTag(tagpair[0], tagpair[1])
				}
				p.AddTag("client_type", "load")
				p.AddFloat64Field("ingest_rate_mean", r.statMapping["*"].Sum/now.Sub(firstStat).Seconds()) /*statMapping["*"].Mean*/
				p.AddFloat64Field("ingest_rate_moving_mean", r.movingAverageStat.Rate())
				p.AddIntField("load_workers", r.Workers)
				telemetrySink <- p
			}
		}

		// print stats to stderr (if printInterval is greater than zero):
		if r.printInterval > 0 && i > 0 && i%r.printInterval == 0 {
			_, err := fmt.Fprintf(os.Stderr, "%s: after %d batches:\n", time.Now().String(), i)
			if err != nil {
				log.Fatal(err)
			}

			r.fprintStats(os.Stderr, r.statMapping)
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
	r.fprintStats(os.Stdout, r.statMapping)
	r.statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func (r *LoadRunner) fprintStats(w io.Writer, statGroups statsMap) {
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
		_, err := fmt.Fprintf(w, "%s : min: %8.2f/s, mean: %8.2f/s, moving mean: %8.2f/s, moving median: %8.2f/s, max: %7.2f/s, count: %8d, sum: %f \n", paddedKey, math.NaN(), v.Sum/time.Now().Sub(firstStat).Seconds(), r.movingAverageStat.Rate(), math.NaN(), math.NaN(), v.Count, v.Sum)
		if err != nil {
			log.Fatal(err)
		}
	}
}
