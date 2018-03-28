// bulk_load_timescale loads a PostgreSQL with TimeScaleDB  with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/jackc/pgx"

	"bytes"
	"context"
	"encoding/gob"
	"io"
)

// TODO VH: This should be calculated from available simulation data
const ValuesPerMeasurement = 11.2222

// Program option vars:
var (
	daemonUrl      string
	workers        int
	batchSize      int
	doLoad         bool
	doDbCreate     bool
	reportDatabase string
	reportHost     string
	reportUser     string
	reportPassword string
	reportTagsCSV  string
	psUser         string
	psPassword     string
	file           string
)

// Global vars
var (
	bufPool        sync.Pool
	batchChan      chan *bytes.Buffer
	batchChanBin   chan []FlatPoint
	batchChanBatch chan []string
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	reportTags     [][2]string
	reportHostname string
	format         string
	reader         *os.File
)

// Output data format choices:
var formatChoices = []string{"timescaledb", "timescaledb-bin", "timescaledb-batch"}

var processes = map[string]struct {
	scan    func(int, io.Reader) (int64, int64)
	process func(*pgx.Conn) int64
}{
	formatChoices[0]: {scan, processBatches},
	formatChoices[1]: {scanBin, processBatchesBin},
	formatChoices[2]: {scanBatch, processBatchesBatch},
}

type FlatPoint struct {
	MeasurementName string
	Columns         []string
	Values          []interface{}
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:5432", "TimeScaleDB URL.")
	flag.StringVar(&psUser, "user", "postgres", "Postgresql user")
	flag.StringVar(&psPassword, "password", "", "Postgresql password")
	flag.StringVar(&file, "file", "", "Input file")

	flag.StringVar(&format, "format", formatChoices[0], "Input data format. One of: "+strings.Join(formatChoices, ","))
	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&doDbCreate, "do-db-create", true, "Whether to create database. Set this flag to false to write data to existing database")

	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")

	flag.Parse()

	if _, ok := processes[format]; !ok {
		log.Fatal("Invalid format choice '", format, "'. Available are: ", strings.Join(formatChoices, ","))
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

	if file != "" {
		if f, err := os.Open(file); err == nil {
			reader = f
		} else {
			log.Fatalf("Error opening %s: %v\n", file, err)
		}
	}
	if reader == nil {
		reader = os.Stdin
	}

}

func main() {
	if doLoad && doDbCreate {
		createDatabase(daemonUrl)
	}

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	batchChan = make(chan *bytes.Buffer, workers)
	batchChanBin = make(chan []FlatPoint, workers)
	batchChanBatch = make(chan []string, workers)
	inputDone = make(chan struct{})

	procs := processes[format]

	procReads := make([]int64, workers)
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		var conn *pgx.Conn
		var err error
		if doLoad {
			hostPort := strings.Split(daemonUrl, ":")
			port, _ := strconv.Atoi(hostPort[1])
			conn, err = pgx.Connect(pgx.ConnConfig{
				Host:     hostPort[0],
				Port:     uint16(port),
				User:     psUser,
				Password: psPassword,
				Database: "measurements",
			})
			if err != nil {
				log.Fatal(err)
			}
		}
		go func(ind int) {
			defer conn.Close()
			procReads[ind] = procs.process(conn)
		}(i)
	}

	start := time.Now()
	itemsRead, bytesRead := procs.scan(batchSize, reader)

	<-inputDone
	close(batchChan)
	close(batchChanBin)
	close(batchChanBatch)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())

	valuesRate := itemsRate * ValuesPerMeasurement
	var totalProcRead int64
	for _, val := range procReads {
		totalProcRead += val
	}

	fmt.Printf("loaded %d items (%d in workers) in %fsec with %d workers (mean point rate %f/sec, mean value rate %f/sec,  %.2fMB/sec from stdin)\n", itemsRead, totalProcRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))
	if file != "" {
		reader.Close()
	}

	if reportHost != "" {

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "TimeScaleDB",
				ReportDatabaseName: reportDatabase,
				ReportHost:         reportHost,
				ReportUser:         reportUser,
				ReportPassword:     reportPassword,
				ReportTags:         reportTags,
				Hostname:           reportHostname,
				DestinationUrl:     daemonUrl,
				Workers:            workers,
				ItemLimit:          -1,
			},
			IsGzip:    false,
			BatchSize: batchSize,
		}
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, -1, took)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads lines from stdin. It expects input in the postgresql sql format.
func scan(itemsPerBatch int, reader io.Reader) (int64, int64) {
	var n int
	var linesRead, bytesRead int64

	buff := bufPool.Get().(*bytes.Buffer)
	newline := []byte("\n")

	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))
	for scanner.Scan() {
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

	// The timescaledb format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead, bytesRead
}

// scan reads lines from stdin. It expects input in the postgresql sql format.
func scanBatch(itemsPerBatch int, reader io.Reader) (int64, int64) {
	var n int
	var linesRead, bytesRead int64

	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))
	var buff = make([]string, 0, itemsPerBatch)
	for scanner.Scan() {
		linesRead++
		line := scanner.Text()
		buff = append(buff, line)
		bytesRead += int64(len(line))
		n++
		if n >= itemsPerBatch {
			batchChanBatch <- buff
			buff = make([]string, 0, itemsPerBatch)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChanBatch <- buff
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	// The timescaledb format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead, bytesRead
}

// scan reads data from stdin. It expects gop encoded points
func scanBin(itemsPerBatch int, reader io.Reader) (int64, int64) {

	var n int
	var itemsRead, bytesRead int64
	var err error
	var lastMeasurement string
	var p FlatPoint

	dec := gob.NewDecoder(reader)
	buff := make([]FlatPoint, 0, itemsPerBatch)

	err = dec.Decode(&p)

	for ; err == nil; err = dec.Decode(&p) {

		//log.Printf("Decoded %d point\n",itemsRead+1)
		newMeasurement := itemsRead > 1 && p.MeasurementName != lastMeasurement
		if !newMeasurement {
			buff = append(buff, p)
			itemsRead++
			n++
		}
		if n > 0 && (n >= itemsPerBatch || newMeasurement) {
			batchChanBin <- buff
			n = 0
			buff = nil
			buff = make([]FlatPoint, 0, itemsPerBatch)
		}
		if newMeasurement {
			buff = append(buff, p)
			itemsRead++
			n++
		}
		lastMeasurement = p.MeasurementName
		p = FlatPoint{}
	}

	if err != nil && err != io.EOF {
		log.Fatalf("Error reading input after %d items: %s", itemsRead, err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChanBin <- buff
		buff = nil
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead, bytesRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(conn *pgx.Conn) int64 {
	var total int64
	for batch := range batchChan {
		if !doLoad {
			continue
		}

		// Write the batch.
		_, err := conn.Exec(string(batch.Bytes()))
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
		total += int64(batch.Len())
	}
	workersGroup.Done()
	return total
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatchesBatch(conn *pgx.Conn) int64 {
	var total int64
	var batches int64
	for batch := range batchChanBatch {
		if !doLoad {
			continue
		}

		// Write the batch.
		sqlBatch := conn.BeginBatch()
		for _, line := range batch {
			sqlBatch.Queue(line, nil, nil, nil)
		}

		err := sqlBatch.Send(context.Background(), nil)

		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}

		for i := 0; i < len(batch); i++ {
			_, err = sqlBatch.ExecResults()
			if err != nil {
				log.Fatalf("Error line %d of batch %d: %s\n", i, batch, err.Error())
			}
		}
		sqlBatch.Close()
		// Return the batch buffer to the pool.
		total += int64(len(batch))
		batches++
	}
	workersGroup.Done()
	return total
}

// CopyFromPoint is implementation of the interface CopyFromSource  used by *Conn.CopyFrom as the source for copy data.
// It wraps arrays of FlatPoints
type CopyFromPoint struct {
	i      int
	points []FlatPoint
	n      int
}

func NewCopyFromPoint(points []FlatPoint) *CopyFromPoint {
	//log.Printf("NewCopyFromPoint\n")
	cp := &CopyFromPoint{}
	cp.points = points
	cp.i = 0
	cp.n = len(points)
	return cp
}

func (c *CopyFromPoint) Next() bool {
	c.i++
	return c.i < c.n
}

func (c *CopyFromPoint) Values() ([]interface{}, error) {
	//log.Printf("Copying %dth values\n",c.i)
	return c.points[c.i].Values, nil
}

func (c *CopyFromPoint) Err() error {
	return nil
}

func (c *CopyFromPoint) Position() int {
	return c.i
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatchesBin(conn *pgx.Conn) int64 {
	n := 0
	var total int64
	for batch := range batchChanBin {
		if !doLoad {
			continue
		}
		//log.Printf("CopyFrom %d of %s\n", n, batch[0].MeasurementName)
		// Write the batch.
		c := NewCopyFromPoint(batch)
		_, err := conn.CopyFrom(pgx.Identifier{batch[0].MeasurementName}, batch[0].Columns, c)
		//log.Println("CopyFrom End")
		if err != nil {
			log.Fatalf("Error writing %d batch of '%s' of size %d in position %d: %s\n", n, batch[0].MeasurementName, len(batch), c.Position(), err.Error())
		}
		total += int64(len(batch))
		n++
	}
	workersGroup.Done()
	return total
}

const createDatabaseSql = "create database measurements;"
const createExtensionSql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

var createTableSql = []string{
	"CREATE table cpu(time bigint not null,hostname TEXT,region TEXT,datacenter TEXT,rack TEXT,os TEXT,arch TEXT,team TEXT,service TEXT,service_version TEXT,service_environment TEXT,usage_user float8,usage_system float8,usage_idle float8,usage_nice float8,usage_iowait float8,usage_irq float8,usage_softirq float8,usage_steal float8,usage_guest float8,usage_guest_nice float8);",
	"CREATE table diskio(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, serial TEXT, reads bigint, writes bigint, read_bytes bigint, write_bytes bigint, read_time bigint, write_time bigint, io_time bigint );",
	"CREATE table disk(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, path TEXT, fstype TEXT, total bigint, free bigint, used bigint, used_percent bigint, inodes_total bigint, inodes_free bigint, inodes_used bigint);",
	"CREATE table kernel(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, boot_time bigint, interrupts bigint, context_switches bigint, processes_forked bigint, disk_pages_in bigint, disk_pages_out bigint);",
	"CREATE table mem(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, total bigint, available bigint, used bigint, free bigint, cached bigint, buffered bigint, used_percent float8, available_percent float8, buffered_percent float8);",
	"CREATE table Net(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, interface TEXT, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint );",
	"CREATE table nginx(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, accepts bigint, active bigint, handled bigint, reading bigint, requests bigint, waiting bigint, writing bigint );",
	"CREATE table postgresl(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, numbackends bigint, xact_commit bigint, xact_rollback bigint, blks_read bigint, blks_hit bigint, tup_returned bigint, tup_fetched bigint, tup_inserted bigint, tup_updated bigint, tup_deleted bigint, conflicts bigint, temp_files bigint, temp_bytes bigint, deadlocks bigint, blk_read_time bigint, blk_write_time bigint );",
	"CREATE table redis(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, uptime_in_seconds bigint, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint, connected_clients bigint, used_memory bigint, used_memory_rss bigint, used_memory_peak bigint, used_memory_lua bigint, rdb_changes_since_last_save bigint, sync_full bigint, sync_partial_ok bigint, sync_partial_err bigint, pubsub_channels bigint, pubsub_patterns bigint, latest_fork_usec bigint, connected_slaves bigint, master_repl_offset bigint, repl_backlog_active bigint, repl_backlog_size bigint, repl_backlog_histlen bigint, mem_fragmentation_ratio bigint, used_cpu_sys bigint, used_cpu_user bigint, used_cpu_sys_children bigint, used_cpu_user_children bigint );",
}

var createHypertableSql = []string{
	"select create_hypertable('cpu','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('diskio','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('disk','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('kernel','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('mem','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('Net','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('nginx','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('postgresl','time', chunk_time_interval => 86400000000000);",
	"select create_hypertable('redis','time', chunk_time_interval => 86400000000000);",
}

var createIndexSql = []string{
	"CREATE index cpu_hostname_index on cpu(hostname);",
}

func createDatabase(daemon_url string) {
	hostPort := strings.Split(daemon_url, ":")
	port, _ := strconv.Atoi(hostPort[1])
	conn, err := pgx.Connect(pgx.ConnConfig{
		Host: hostPort[0],
		Port: uint16(port),
		User: psUser,
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec(createDatabaseSql)
	conn.Close()
	if err != nil {
		log.Fatal(err)
	}
	conn, err = pgx.Connect(pgx.ConnConfig{
		Host:     hostPort[0],
		Port:     uint16(port),
		User:     psUser,
		Database: "measurements",
	})

	defer func() {
		conn.Close()
	}()
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec(createExtensionSql)
	if err != nil {
		log.Fatal(err)
	}
	for _, sql := range createTableSql {
		_, err = conn.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range createIndexSql {
		_, err = conn.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range createHypertableSql {
		_, err = conn.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
	}

}
