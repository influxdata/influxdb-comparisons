// bulk_load_opentsdb loads an OpenTSDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/klauspost/compress/gzip"
)

type OpenTsdbBulkLoad struct {
	// Program option vars:
	csvDaemonUrls string
	daemonUrls    []string
	backoff       time.Duration

	// Global vars
	bufPool        sync.Pool
	batchChan      chan *bytes.Buffer
	inputDone      chan struct{}
	backingOffChan chan bool
	backingOffDone chan struct{}
	valuesRead     int64
	itemsRead      int64
	bytesRead      int64
	scanFinished   bool
}

var load = &OpenTsdbBulkLoad{}

// Parse args:
func init() {
	bulk_load.Runner.Init(5000)
	load.Init()

	flag.Parse()

	bulk_load.Runner.Validate()
	load.Validate()

}

func main() {
	bulk_load.Runner.Run(load)
}

func (l *OpenTsdbBulkLoad) Init() {
	flag.StringVar(&l.csvDaemonUrls, "urls", "http://localhost:8086", "OpenTSDB URLs, comma-separated. Will be used in a round-robin fashion.")
	//flag.DurationVar(&l.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
}

func (l *OpenTsdbBulkLoad) Validate() {
	l.daemonUrls = strings.Split(l.csvDaemonUrls, ",")
	if len(l.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", l.daemonUrls)
}

func (l *OpenTsdbBulkLoad) CreateDb() {
	// check that there are no pre-existing databases:
	existingDatabases, err := listDatabases(l.daemonUrls[0])
	if err != nil {
		log.Fatal(err)
	}

	if len(existingDatabases) > 0 {
		log.Fatalf("There are databases already in the data store. If you know what you are doing, run the command:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", existingDatabases[0])
	}
}

func (l *OpenTsdbBulkLoad) PrepareWorkers() {
	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan *bytes.Buffer, bulk_load.Runner.Workers)
	l.inputDone = make(chan struct{})

	l.backingOffChan = make(chan bool, 100)
	l.backingOffDone = make(chan struct{})

	go l.processBackoffMessages()
}

func (l *OpenTsdbBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *OpenTsdbBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *OpenTsdbBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *OpenTsdbBulkLoad) CleanUp() {
	close(l.backingOffChan)
	<-l.backingOffDone
}

func (l *OpenTsdbBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {
	params.DBType = "OpenTSDB"
	params.DestinationUrl = l.daemonUrls[0]
	params.IsGzip = true
	return
}

func (l *OpenTsdbBulkLoad) PrepareProcess(i int) {

}

func (l *OpenTsdbBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	daemonUrl := l.daemonUrls[i%len(l.daemonUrls)]
	cfg := HTTPWriterConfig{
		Host: daemonUrl,
	}
	return l.processBatches(NewHTTPWriter(cfg), waitGroup)
}

func (l *OpenTsdbBulkLoad) AfterRunProcess(i int) {

}

func (l *OpenTsdbBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *OpenTsdbBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *OpenTsdbBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

// scan reads one line at a time from stdin.
// When the requested number of lines per batch is met, send a batch over batchChan for the workers to write.
func (l *OpenTsdbBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0
	buf := l.bufPool.Get().(*bytes.Buffer)
	zw := gzip.NewWriter(buf)

	var n int

	openbracket := []byte("[")
	closebracket := []byte("]")
	commaspace := []byte(", ")
	newline := []byte("\n")

	zw.Write(openbracket)
	zw.Write(newline)

	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 4*1024*1024))

	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for scanner.Scan() {
		l.itemsRead++
		if n > 0 {
			zw.Write(commaspace)
			zw.Write(newline)
		}

		zw.Write(scanner.Bytes())

		n++
		if n >= bulk_load.Runner.BatchSize {
			zw.Write(newline)
			zw.Write(closebracket)
			zw.Close()

			l.batchChan <- buf

			buf = l.bufPool.Get().(*bytes.Buffer)
			zw = gzip.NewWriter(buf)
			zw.Write(openbracket)
			zw.Write(newline)
			n = 0
			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}

		}
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		zw.Write(newline)
		zw.Write(closebracket)
		zw.Close()
		l.batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)
	l.scanFinished = true
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *OpenTsdbBulkLoad) processBatches(w LineProtocolWriter, workersGroup *sync.WaitGroup) error {
	var rerr error
	for batch := range l.batchChan {
		// Write the batch: try until backoff is not needed.
		if bulk_load.Runner.DoLoad {
			var err error
			for {
				_, err = w.WriteLineProtocol(batch.Bytes())
				if err == BackoffError {
					l.backingOffChan <- true
					time.Sleep(l.backoff)
				} else {
					l.backingOffChan <- false
					break
				}
			}
			if err != nil {
				rerr = fmt.Errorf("Error writing: %s\n", err.Error())
			}
		}
		//fmt.Println(string(batch.Bytes()))

		// Return the batch buffer to the pool.
		batch.Reset()
		l.bufPool.Put(batch)
	}
	workersGroup.Done()
	return rerr
}

func (l *OpenTsdbBulkLoad) processBackoffMessages() {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range l.backingOffChan {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("backoff took %.02fsec\n", took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("backoffs took a total of %fsec of runtime\n", totalBackoffSecs)
	l.backingOffDone <- struct{}{}
}

// TODO(rw): listDatabases lists the existing data in OpenTSDB.
func listDatabases(daemonUrl string) ([]string, error) {
	return nil, nil
}
