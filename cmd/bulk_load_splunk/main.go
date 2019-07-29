// bulk_load_influx loads an InfluxDB daemon with data from stdin.

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
	"strconv"
)

// Program option vars:
var (
	inputUrl               string
	authToken              string
	workers                int
	batchSize              int
	doLoad                 bool
	useGzip                bool
	memprofile             bool
	cpuProfileFile         string
	reportDatabase         string
	reportHost             string
	reportUser             string
	reportPassword         string
	reportTagsCSV          string
	printInterval          uint64
)

// Global vars
var (
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	workersGroup          sync.WaitGroup
	syncChanDone          chan int
	reportTags            [][2]string
	reportHostname        string
	endedPrematurely      bool
	prematureEndReason    string
	scanFinished          bool
)

type batch struct {
	Buffer *bytes.Buffer
	Items  int
}

// Parse args:
func init() {
	flag.StringVar(&inputUrl, "url", "http://localhost:8100", "Splunk input URL.")
	flag.StringVar(&authToken, "auth-token", "", "Data input authorization token.")
	flag.IntVar(&batchSize, "batch-size", 5000, "Batch size.")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&memprofile, "memprofile", false, "Whether to write a memprofile (file automatically determined).")
	flag.StringVar(&cpuProfileFile, "cpu-profile", "", "Write cpu profile to `file`")
	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")
	flag.Uint64Var(&printInterval, "print-interval", 1000, "Print timing stats to stderr after this many batches (0 to disable)")

	flag.Parse()

	fmt.Printf("Splunk input URL: %v\n", inputUrl)

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

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan batch, workers)
	inputDone = make(chan struct{})
	syncChanDone = make(chan int)

	var once sync.Once

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("Worker #%d, dest url: %s", i, inputUrl),
			Host:           inputUrl,
			Token:          authToken,
		}
		go func(w int) {
			err := processBatches(NewHTTPWriter(cfg))
			if err != nil {
				fmt.Printf("%+v\n", err)
				once.Do(func() {
					endedPrematurely = true
					prematureEndReason = "Worker error"
					if !scanFinished {
						go func() {
							for range batchChan {
								//read out remaining batches
							}
						}()
						syncChanDone <- 1
					}
					exitCode = 1
				})
			}
		}(i)
	}
	fmt.Printf("Started load with %d workers\n", workers)

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scan(batchSize, syncChanDone)

	<-inputDone
	close(batchChan)
	close(syncChanDone)

	workersGroup.Wait()

	end := time.Now()
	took := end.Sub(start)

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	if endedPrematurely {
		fmt.Printf("load finished prematurely: %s\n", prematureEndReason)
	}

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		if endedPrematurely {
			reportTags = append(reportTags, [2]string{"premature_end_reason", report.Escape(prematureEndReason)})
		}

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "Splunk",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     inputUrl,
				Workers:            workers,
			},
			IsGzip:    useGzip,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took)

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
	var n int
	var linesRead, bytesRead int64
	var totalValues int64

	newline := []byte("\n")
	buf := bufPool.Get().(*bytes.Buffer)
	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4*1024*1024))

outer:
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, common.DatasetSizeMarker) {
			parts := common.DatasetSizeMarkerRE.FindAllStringSubmatch(line, -1)
			if parts == nil || len(parts[0]) != 3 {
				log.Fatalf("Incorrent number of matched groups: %#v", parts)
			}
			if i, err := strconv.Atoi(parts[0][1]); err == nil {
				_ = int64(i)
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
		linesRead++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= itemsPerBatch {
			bytesRead += int64(buf.Len())
			batchChan <- batch{buf, n}
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0
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

	if linesRead != /*totalPoints*/totalValues { // Splunk protocol has one value per input line ie. JSON metric item
		if !endedPrematurely {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", linesRead, /*totalPoints*/totalValues)
		}
	}

	// The splunk format uses 1 value per item
	itemsRead := linesRead

	scanFinished = true

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(w *HTTPWriter) error {
	var batchesSeen int64

	defer workersGroup.Done()

	for batch := range batchChan {
		batchesSeen++

		// Write the batch: try until backoff is not needed.
		if doLoad {
			var err error
			if useGzip {
				compressedBatch := bufPool.Get().(*bytes.Buffer)
				fasthttp.WriteGzip(compressedBatch, batch.Buffer.Bytes())
				//bodySize = len(compressedBatch.Bytes())
				_, err = w.WriteJsonProtocol(compressedBatch.Bytes(), true)
				// Return the compressed batch buffer to the pool.
				compressedBatch.Reset()
				bufPool.Put(compressedBatch)
			} else {
				//bodySize = len(batch.Bytes())
				_, err = w.WriteJsonProtocol(batch.Buffer.Bytes(), false)
			}
			if err != nil {
				return err // fmt.Errorf("Error writing: %s\n", err.Error())
			}
		}

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		bufPool.Put(batch.Buffer)
	}

	return nil
}