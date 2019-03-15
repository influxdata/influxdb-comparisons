// bulk_load_graphite loads Graphite/Carbon with data from stdin or file.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/kisielk/og-rek"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Program option vars:
var (
	carbonUrl           string
	graphiteUrl         string
	workers             int
	batchSize           int
	backoff             time.Duration
	stallThreshold      time.Duration
	doLoad              bool
	reportDatabase      string
	reportHost          string
	reportUser          string
	reportPassword      string
	reportTagsCSV       string
	file                string
)

// Global vars
var (
	bufPool        sync.Pool
	batchChan      chan *bytes.Buffer
	batchChanLines chan []string
	inputDone      chan struct{}
	reportTags     [][2]string
	reportHostname string
	format         string
	sourceReader   *os.File
)

// Output data format choices:
var formatChoices = []string{"graphite-line", "graphite-line2pickle" }

var processes = map[string]struct {
	scan    func(int, io.Reader) (int64, int64, int64)
	process func(net.Conn) int64
}{
	formatChoices[0]:           {scan, processBatches},
	formatChoices[1]:           {scanLine, processTupleBatches},
}

// Parse args:
func init() {
	flag.StringVar(&carbonUrl, "carbon-url", "localhost:2003", "Carbon-cache or carbon-relay host:port.")
	flag.StringVar(&graphiteUrl, "url", "http://localhost:8080", "Graphite URL.")
	flag.StringVar(&file, "file", "", "Input file")

	flag.StringVar(&format, "format", formatChoices[0], "Input data format. One of: "+strings.Join(formatChoices, ","))
	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.DurationVar(&backoff, "backoff", 1*time.Second, "Time to sleep between requests when server indicates stall is needed.")
	flag.DurationVar(&stallThreshold, "stall-threshold", 100*time.Millisecond, "Amount of time that represents relay stall.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")

	flag.Parse()

	if _, ok := processes[format]; !ok {
		log.Fatal("Invalid format choice '", format, "'. Available are: ", strings.Join(formatChoices, ","))
	}

	log.Printf("relay stall time: %v, backoff: %v", stallThreshold, backoff)

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

	if file != "" {
		if f, err := os.Open(file); err == nil {
			sourceReader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", file, err)
		}
	}
	if sourceReader == nil {
		sourceReader = os.Stdin
	}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	// override pool type for some formats
	switch format {
	case formatChoices[1]:
		bufPool = sync.Pool{
			New: func() interface{} {
				return make([]string, 0, batchSize)
			},
		}
	}


	batchChan = make(chan *bytes.Buffer, workers)
	batchChanLines = make(chan []string, workers)
	inputDone = make(chan struct{})

	var workersGroup sync.WaitGroup
	procs := processes[format]
	procReads := make([]int64, workers)

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		var conn net.Conn
		var err error
		if doLoad {
			conn, err = net.Dial("tcp", carbonUrl)
			if err != nil {
				log.Fatal(err)
			}
			tcp, _ := conn.(*net.TCPConn)
			if err = tcp.SetKeepAlive(true); err != nil {
				log.Printf("failed to set TCP keep-alive: %v\n", err)
			}
		}
		go func(ind int, connection net.Conn) {
			defer func() {
				// do not signal we're done until all data has been sent... but how to ensure it?
				if doLoad {
					t0 := time.Now()
					tcp, _ := connection.(*net.TCPConn)
					if err := tcp.CloseWrite(); err != nil { // == shutdown(WR)
						log.Printf("failed to shutdown socket: %v\n", err)
					}
					if err := connection.Close(); err != nil {
						log.Printf("failed to close connection: %v\n", err)
					}
					// paranoid check if shutdown did not take too long (>1s) to affect result ingest rate
					dt := time.Now().Sub(t0).Seconds()
					if dt > 1 {
						log.Fatalf("connection shutdown took %f seconds\n", dt)
					}
				}
				workersGroup.Done()
			}()
			procReads[ind] = procs.process(connection)
		}(i, conn)
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := procs.scan(batchSize, sourceReader)
	<-inputDone
	close(batchChan)
	close(batchChanLines)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/sec,  %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))
	if file != "" {
		sourceReader.Close()
	}

	if reportHost != "" && doLoad {
		reportTags = append(reportTags, [2]string{"format", format})
		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "Graphite",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     carbonUrl,
				Workers:            workers,
				ItemLimit:          -1,
			},
			IsGzip:    false,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads lines from stdin. It expects input in Carbon plaintext format.
func scan(itemsPerBatch int, reader io.Reader) (int64, int64, int64) {
	var n int
	var linesRead, bytesRead int64
	var totalValues int64

	buff := bufPool.Get().(*bytes.Buffer)
	newline := []byte("\n")
	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))

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

		buff.Write(scanner.Bytes())
		buff.Write(newline)

		n++
		if n >= itemsPerBatch {
			bytesRead += int64(buff.Len())
			batchChan <- buff
			buff = bufPool.Get().(*bytes.Buffer)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- buff
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	if linesRead != /*totalPoints*/totalValues { // Graphite line protocol has one value per line
		log.Fatalf("Incorrent number of read points: %d, expected: %d:", linesRead, /*totalPoints*/totalValues)
	}

	// The graphite format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(conn net.Conn) int64 {
	var total int64
	for batch := range batchChan {
		if !doLoad {
			continue
		}

		// Write the batch.
		t0 := time.Now()
		_, err := conn.Write(batch.Bytes())
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
		dt := time.Now().Sub(t0)
		if dt >= stallThreshold {
			log.Printf("Relay stalled; %d ms [%s -> %s]", dt/time.Millisecond, conn.LocalAddr().String(), conn.RemoteAddr().String())
			time.Sleep(backoff)
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
		total += int64(batch.Len())
	}

	return total
}

// scan reads lines from stdin. It expects input in Carbon plaintext format.
func scanLine(itemsPerBatch int, reader io.Reader) (int64, int64, int64) {
	var n int
	var linesRead, bytesRead int64
	var totalValues int64

	buff := bufPool.Get().([]string)
	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))

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
		bytesRead += int64(len(line))
		buff = append(buff, line)

		n++
		if n >= itemsPerBatch {
			batchChanLines <- buff
			buff = bufPool.Get().([]string)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChanLines <- buff
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	if linesRead != /*totalPoints*/totalValues { // Graphite line protocol has one value per line
		log.Fatalf("Incorrent number of read points: %d, expected: %d:", linesRead, /*totalPoints*/totalValues)
	}

	// The graphite format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChanTuple and writes them to the target server, while tracking stats on the write.
func processTupleBatches(conn net.Conn) int64 {
	var total int64

	tuples := make([]interface{}, 0)
	header := make([]byte, 4)
	buf := &bytes.Buffer{}
	enc := ogórek.NewEncoderWithConfig(buf, &ogórek.EncoderConfig{
		Protocol: 2,
	})

	for batch := range batchChanLines {
		if !doLoad {
			continue
		}

		// Create tuple list
		tuples = tuples[:0]
		for _,line := range batch {
			parts := strings.Split(line, " ")
			name := parts[0]
			timestamp, _ := strconv.Atoi(parts[2])
			value, err := getValue(parts[1])
			if err != nil {
				log.Fatalf("error parsing line [%s]: %v", line, err)
			}
			tuple := &ogórek.Tuple{ name, ogórek.Tuple{ timestamp, value } }
			tuples = append(tuples, tuple)
		}

		// Write pickle
		buf.Reset()
		err := enc.Encode(tuples)
		if err != nil {
			log.Fatalf("error encoding tuple: %v\n", err)
		}
		payload := buf.Bytes()
		binary.BigEndian.PutUint32(header, uint32(len(payload))) // struct.pack("!L", len(payload))
		_, err = conn.Write(header)
		if err != nil {
			log.Fatalf("Error writing header: %v\n", err)
		}
		_, err = conn.Write(payload)
		if err != nil {
			log.Fatalf("Error writing payload: %v\n", err)
		}

		// Return the batch buffer to the pool.
		batch = batch[:0]
		bufPool.Put(batch)
		total += int64(len(payload))
	}

	return total
}

func getValue(s string) (interface{}, error) {
	v_int32, err := strconv.ParseInt(s, 10, 32)
	if err == nil {
		return int32(v_int32), nil
	}
	v_int64, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return int32(v_int64), nil
	}
	v_float32, err := strconv.ParseFloat(s, 32)
	if err == nil {
		return v_float32, nil
	}
	v_float64, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return v_float64, nil
	}
	return nil, fmt.Errorf("unsupported value '%s'", s)
}