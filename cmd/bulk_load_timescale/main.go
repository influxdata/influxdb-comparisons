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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	timescale_serialization "github.com/influxdata/influxdb-comparisons/timescale_serializaition"
)

// Output data format choices:
var formatChoices = []string{"timescaledb-sql", "timescaledb-copyFrom"}

type procInfo struct {
	scan    func(*TimescaleBulkLoad, io.Reader, chan int)
	process func(*TimescaleBulkLoad, *pgx.Conn, *sync.WaitGroup) error
}

var processes = map[string]procInfo{
	formatChoices[0]:           {(*TimescaleBulkLoad).scan, (*TimescaleBulkLoad).processBatches},
	formatChoices[1]:           {(*TimescaleBulkLoad).scanBin, (*TimescaleBulkLoad).processBatchesBin},
	"timescaledb-sql-batching": {(*TimescaleBulkLoad).scanBatch, (*TimescaleBulkLoad).processBatchesBatch},
}

type FlatPoint struct {
	MeasurementName string
	Columns         []string
	Values          []interface{}
}

// TODO VH: This should be calculated from available simulation data
const ValuesPerMeasurement = 11.2222
const DatabaseName = "benchmark_db"

type TimescaleBulkLoad struct {
	// Program option vars:
	daemonUrl  string
	psUser     string
	psPassword string

	chunkDuration       time.Duration
	usePostgresBatching bool
	// Global vars
	bufPool          sync.Pool
	batchChan        chan *bytes.Buffer
	batchChanBin     chan []FlatPoint
	batchChanBatch   chan []string
	inputDone        chan struct{}
	format           string
	formatProcessors procInfo
	valuesRead       int64
	itemsRead        int64
	bytesRead        int64
	scanFinished     bool
	pool             *pgxpool.Pool
}

var load = &TimescaleBulkLoad{}

// Parse args:
func init() {
	bulk_load.Runner.Init(100)
	load.Init()

	flag.Parse()

	bulk_load.Runner.Validate()
	load.Validate()

}

func main() {
	bulk_load.Runner.Run(load)
}

func (l *TimescaleBulkLoad) Init() {
	flag.StringVar(&l.daemonUrl, "url", "localhost:5432", "Timescale DB URL.")
	flag.StringVar(&l.psUser, "user", "postgres", "Postgresql user")
	flag.StringVar(&l.psPassword, "password", "", "Postgresql password")
	flag.StringVar(&l.format, "format", formatChoices[1], "Input data format. One of: "+strings.Join(formatChoices, ","))
	flag.BoolVar(&l.usePostgresBatching, "postgresql-batching", false, "Whether to use Postgresql batching feature. Works only for '"+formatChoices[0]+"' format")
	flag.DurationVar(&l.chunkDuration, "chunk-interval", time.Hour*24, "Timescale chunk interval")
}

func (l *TimescaleBulkLoad) Validate() {
	if _, ok := processes[l.format]; !ok {
		log.Fatal("Invalid format choice '", l.format, "'. Available are: ", strings.Join(formatChoices, ","))
	}
	if l.usePostgresBatching {
		if l.format == formatChoices[1] {
			log.Fatal("Cannot use Postgresql batching when using format '", formatChoices[1], "'")
		} else {
			l.format = "timescaledb-sql-batching"
		}
	}
}

func (l *TimescaleBulkLoad) CreateDb() {
	l.createDatabase(l.daemonUrl)
}

func (l *TimescaleBulkLoad) PrepareWorkers() {
	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan *bytes.Buffer, bulk_load.Runner.Workers)
	l.batchChanBin = make(chan []FlatPoint, bulk_load.Runner.Workers)
	l.batchChanBatch = make(chan []string, bulk_load.Runner.Workers)
	l.inputDone = make(chan struct{})

	l.formatProcessors = processes[l.format]

}

func (l *TimescaleBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *TimescaleBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *TimescaleBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
	close(l.batchChanBin)
	close(l.batchChanBatch)
}

func (l *TimescaleBulkLoad) CleanUp() {

}

func (l *TimescaleBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {
	reportTags = [][2]string{{"format", l.format}}
	reportTags = append(reportTags, [2]string{"postgresql_batching", strconv.FormatBool(l.usePostgresBatching)})
	reportTags = append(reportTags, [2]string{"chunk_interval", l.chunkDuration.String()})
	params.DBType = "TimeScaleDB"
	params.DestinationUrl = l.daemonUrl
	return
}

func (l *TimescaleBulkLoad) PrepareProcess(i int) {

}

func (l *TimescaleBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	var conn *pgx.Conn
	var err error
	if bulk_load.Runner.DoLoad {
		//# Example DSN
		//user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=password database=benchmark_db", "localhost", uint16(5432), l.psUser)
		fmt.Println("*****", dsn)
		config, err := pgx.ParseConfig(dsn)
		if err != nil {
			log.Fatal(err)
		}
		conn, err = pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = l.formatProcessors.process(l, conn, waitGroup)

	if bulk_load.Runner.DoLoad {
		conn.Close(context.Background())
	}
	return err
}

func (l *TimescaleBulkLoad) AfterRunProcess(i int) {

}

func (l *TimescaleBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *TimescaleBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	l.formatProcessors.scan(l, r, syncChanDone)
}

func (l *TimescaleBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *TimescaleBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

// scan reads lines from stdin. It expects input in the postgresql sql format.
func (l *TimescaleBulkLoad) scan(reader io.Reader, syncChanDone chan int) {
	var n int
	var totalPoints, totalValues int64
	var err error

	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0

	buff := l.bufPool.Get().(*bytes.Buffer)
	newline := []byte("\n")

	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for scanner.Scan() {
		totalPoints, totalValues, err = common.CheckTotalValues(scanner.Text())
		if totalPoints > 0 || totalValues > 0 {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}
		l.itemsRead++

		buff.Write(scanner.Bytes())
		buff.Write(newline)

		n++
		if n >= bulk_load.Runner.BatchSize {
			l.bytesRead += int64(buff.Len())
			l.batchChan <- buff
			buff = l.bufPool.Get().(*bytes.Buffer)
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
		l.batchChan <- buff
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	l.valuesRead = totalValues
	if l.itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", l.itemsRead, totalPoints)
		} else {
			totalValues = int64(float64(l.itemsRead) * bulk_load.ValuesPerMeasurement) // needed for statistics summary
		}
	}
	l.scanFinished = true

}

// scan reads lines from stdin. It expects input in the postgresql sql format.
func (l *TimescaleBulkLoad) scanBatch(reader io.Reader, syncChanDone chan int) {
	var n int
	var err error
	var totalPoints, totalValues int64
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0

	scanner := bufio.NewScanner(bufio.NewReaderSize(reader, 4*1024*1024))
	var buff = make([]string, 0, bulk_load.Runner.BatchSize)
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for scanner.Scan() {
		line := scanner.Text()

		totalPoints, totalValues, err = common.CheckTotalValues(line)
		if totalPoints > 0 || totalValues > 0 {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}

		l.itemsRead++
		buff = append(buff, line)
		l.bytesRead += int64(len(line))
		n++
		if n >= bulk_load.Runner.BatchSize {
			l.batchChanBatch <- buff
			buff = make([]string, 0, bulk_load.Runner.BatchSize)
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
		l.batchChanBatch <- buff
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	if l.itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", l.itemsRead, totalPoints)
		} else {
			totalValues = int64(float64(l.itemsRead) * bulk_load.ValuesPerMeasurement) // needed for statistics summary
		}
	}
	l.valuesRead = totalValues
	l.scanFinished = true
}

// scan reads data from stdin. It expects gop encoded points
func (l *TimescaleBulkLoad) scanBin(reader io.Reader, syncChanDone chan int) {

	var n int
	var err error
	var lastMeasurement string
	var p FlatPoint
	var tsfp timescale_serialization.FlatPoint
	var size uint64

	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0

	buff := make([]FlatPoint, 0, bulk_load.Runner.BatchSize)
	byteBuff := make([]byte, 100*1024)
	buffReader := bufio.NewReaderSize(reader, 4*1024*1024)
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for {
		err = binary.Read(buffReader, binary.LittleEndian, &size)
		fmt.Println("********** size = ", size)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("cannot read size of %d item: %v\n", l.itemsRead, err)
		}

		if uint64(cap(byteBuff)) < size {
			byteBuff = make([]byte, size)
		}

		bytesPerItem := uint64(0)
		for i := 10; i > 0; i-- {
			r, err := reader.Read(byteBuff[bytesPerItem:size])
			if err != nil && err != io.EOF {
				log.Fatalf("cannot read %d item: %v\n", l.itemsRead, err)
			}
			bytesPerItem += uint64(r)
			if bytesPerItem == size {
				break
			}

		}
		if bytesPerItem != size {
			log.Fatalf("cannot read %d item: read %d, expected %d\n", l.itemsRead, bytesPerItem, size)
		}
		err = tsfp.Unmarshal(byteBuff[:size])
		if err != nil {
			log.Fatalf("cannot unmarshall %d item: %v\n", l.itemsRead, err)
		}

		l.bytesRead += int64(size) + 8

		p.MeasurementName = tsfp.MeasurementName
		p.Columns = tsfp.Columns
		p.Values = make([]interface{}, len(tsfp.Values))
		for i, f := range tsfp.Values {
			switch f.Type {
			case timescale_serialization.FlatPoint_FLOAT:
				p.Values[i] = f.DoubleVal
				break
			case timescale_serialization.FlatPoint_INTEGER:
				p.Values[i] = f.IntVal
				break
			case timescale_serialization.FlatPoint_STRING:
				p.Values[i] = f.StringVal
				break
			default:
				log.Fatalf("invalid type of %d item: %d", l.itemsRead, f.Type)
			}
		}
		l.valuesRead += int64(len(tsfp.Values))

		//log.Printf("Decoded %d point\n",itemsRead+1)
		newMeasurement := l.itemsRead > 1 && p.MeasurementName != lastMeasurement
		if !newMeasurement {
			buff = append(buff, p)
			l.itemsRead++
			n++
		}
		if n > 0 && (n >= bulk_load.Runner.BatchSize || newMeasurement) {
			l.batchChanBin <- buff
			n = 0
			buff = nil
			buff = make([]FlatPoint, 0, bulk_load.Runner.BatchSize)
			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}
		}
		if newMeasurement {
			buff = append(buff, p)
			l.itemsRead++
			n++
		}
		lastMeasurement = p.MeasurementName
		p = FlatPoint{}
		tsfp = timescale_serialization.FlatPoint{}
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	if err != nil && err != io.EOF {
		log.Fatalf("Error reading input after %d items: %s", l.itemsRead, err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		l.batchChanBin <- buff
		buff = nil
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)
	l.scanFinished = true
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *TimescaleBulkLoad) processBatches(conn *pgx.Conn, workersGroup *sync.WaitGroup) error {
	var rerr error
	for batch := range l.batchChan {
		if !bulk_load.Runner.DoLoad {
			continue
		}

		// Write the batch.
		_, err := conn.Exec(context.Background(), string(batch.Bytes()))
		if err != nil {
			rerr = fmt.Errorf("Error writing: %s\n", err.Error())
			break
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		l.bufPool.Put(batch)
	}
	workersGroup.Done()
	return rerr
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *TimescaleBulkLoad) processBatchesBatch(conn *pgx.Conn, workersGroup *sync.WaitGroup) error {
	var batches int64
	var rerr error
	for batch := range l.batchChanBatch {
		if !bulk_load.Runner.DoLoad {
			continue
		}

		// Write the batch.
		sqlBatch := pgx.Batch{}
		for _, line := range batch {
			sqlBatch.Queue(line, nil, nil, nil)
		}

		sqlBatchResults := l.pool.SendBatch(context.Background(), &sqlBatch)
		if err := sqlBatchResults.Close(); err != nil {
			log.Fatalf("failed to close a batch operation %v", err)
		}
		batches++
	}
	workersGroup.Done()
	return rerr
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
	cp.i = -1
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
func (l *TimescaleBulkLoad) processBatchesBin(conn *pgx.Conn, workersGroup *sync.WaitGroup) error {
	n := 0
	var rerr error
	for batch := range l.batchChanBin {
		if !bulk_load.Runner.DoLoad {
			continue
		}
		//log.Printf("CopyFrom %d of %s\n", n, batch[0].MeasurementName)
		// Write the batch.
		c := NewCopyFromPoint(batch)
		rows, err := conn.CopyFrom(context.Background(), pgx.Identifier{batch[0].MeasurementName}, batch[0].Columns, c)
		//log.Println("CopyFrom End")
		if err != nil {
			rerr = fmt.Errorf("Error writing %d batch of '%s' of size %d in position %d: %s\n", n, batch[0].MeasurementName, len(batch), c.Position(), err.Error())
			break
		}
		if rows != int64(len(batch)) {
			rerr = fmt.Errorf("Problem writing of %d batch: Written only %d rows of %d", n, rows, len(batch))
			break
		}
		n++
	}
	workersGroup.Done()
	return rerr
}

const createDatabaseSql = "create database " + DatabaseName + ";"
const createExtensionSql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

var DevopsCreateTableSql = []string{
	"CREATE table cpu(time bigint not null,hostname TEXT,region TEXT,datacenter TEXT,rack TEXT,os TEXT,arch TEXT,team TEXT,service TEXT,service_version TEXT,service_environment TEXT,usage_user float8,usage_system float8,usage_idle float8,usage_nice float8,usage_iowait float8,usage_irq float8,usage_softirq float8,usage_steal float8,usage_guest float8,usage_guest_nice float8);",
	"CREATE table diskio(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, serial TEXT, reads bigint, writes bigint, read_bytes bigint, write_bytes bigint, read_time bigint, write_time bigint, io_time bigint );",
	"CREATE table disk(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, path TEXT, fstype TEXT, total bigint, free bigint, used bigint, used_percent bigint, inodes_total bigint, inodes_free bigint, inodes_used bigint);",
	"CREATE table kernel(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, boot_time bigint, interrupts bigint, context_switches bigint, processes_forked bigint, disk_pages_in bigint, disk_pages_out bigint);",
	"CREATE table mem(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, total bigint, available bigint, used bigint, free bigint, cached bigint, buffered bigint, used_percent float8, available_percent float8, buffered_percent float8);",
	"CREATE table net(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, interface TEXT, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint, bytes_sent bigint, bytes_recv bigint, packets_sent bigint, packets_recv bigint, err_in bigint, err_out bigint, drop_in bigint, drop_out bigint );",
	"CREATE table nginx(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, accepts bigint, active bigint, handled bigint, reading bigint, requests bigint, waiting bigint, writing bigint );",
	"CREATE table postgresl(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, numbackends bigint, xact_commit bigint, xact_rollback bigint, blks_read bigint, blks_hit bigint, tup_returned bigint, tup_fetched bigint, tup_inserted bigint, tup_updated bigint, tup_deleted bigint, conflicts bigint, temp_files bigint, temp_bytes bigint, deadlocks bigint, blk_read_time bigint, blk_write_time bigint );",
	"CREATE table redis(time bigint not null, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, uptime_in_seconds bigint, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint, connected_clients bigint, used_memory bigint, used_memory_rss bigint, used_memory_peak bigint, used_memory_lua bigint, rdb_changes_since_last_save bigint, sync_full bigint, sync_partial_ok bigint, sync_partial_err bigint, pubsub_channels bigint, pubsub_patterns bigint, latest_fork_usec bigint, connected_slaves bigint, master_repl_offset bigint, repl_backlog_active bigint, repl_backlog_size bigint, repl_backlog_histlen bigint, mem_fragmentation_ratio bigint, used_cpu_sys bigint, used_cpu_user bigint, used_cpu_sys_children bigint, used_cpu_user_children bigint );",
}

var IotCreateTableSql = []string{
	"CREATE TABLE air_quality_room (time bigint not null,room_id TEXT,sensor_id TEXT,home_id TEXT, co2_level float8,co_level float8,battery_voltage float8 )",
	"CREATE TABLE air_condition_room (time bigint not null,room_id TEXT,sensor_id TEXT,home_id TEXT, temperature float8,humidity float8,battery_voltage float8 )",
	"CREATE TABLE air_condition_outdoor (time bigint not null,sensor_id TEXT,home_id TEXT, temperature float8,humidity float8,battery_voltage float8 )",
	"CREATE TABLE camera_detection (time bigint not null,sensor_id TEXT,home_id TEXT, object_type TEXT,object_kind TEXT,battery_voltage float8 )",
	"CREATE TABLE door_state (time bigint not null,door_id TEXT,sensor_id TEXT,	home_id TEXT, state float8,battery_voltage float8 )",
	"CREATE TABLE home_config (time bigint not null,sensor_id TEXT,home_id TEXT, config_string TEXT)",
	"CREATE TABLE home_state (time bigint not null,sensor_id TEXT,home_id TEXT, state BIGINT,state_string TEXT)",
	"CREATE TABLE light_level_room (time bigint not null,room_id TEXT,sensor_id TEXT,home_id TEXT, level float8,battery_voltage float8 )",
	"CREATE TABLE radiator_valve_room (time bigint not null,room_id TEXT,radiator TEXT,sensor_id TEXT,home_id TEXT, opening_level float8,battery_voltage float8 )",
	"CREATE TABLE water_leakage_room (time bigint not null,sensor_id TEXT,room_id TEXT,home_id TEXT, leakage float8,battery_voltage float8 )",
	"CREATE TABLE water_level (time bigint not null,sensor_id TEXT,home_id TEXT, level float8,battery_voltage float8 )",
	"CREATE TABLE weather_outdoor (time bigint not null,sensor_id TEXT,home_id TEXT, pressure float8,wind_speed float8,wind_direction float8,precipitation float8,battery_voltage float8 )",
	"CREATE TABLE window_state_room (time bigint not null,room_id TEXT,sensor_id TEXT,window_id TEXT,home_id TEXT, state float8,battery_voltage float8 )",
}

var devopsCreateHypertableSql = []string{
	"select create_hypertable('cpu','time', chunk_time_interval => %d);",
	"select create_hypertable('diskio','time', chunk_time_interval => %d);",
	"select create_hypertable('disk','time', chunk_time_interval => %d);",
	"select create_hypertable('kernel','time', chunk_time_interval => %d);",
	"select create_hypertable('mem','time', chunk_time_interval => %d);",
	"select create_hypertable('Net','time', chunk_time_interval => %d);",
	"select create_hypertable('nginx','time', chunk_time_interval => %d);",
	"select create_hypertable('postgresl','time', chunk_time_interval => %d);",
	"select create_hypertable('redis','time', chunk_time_interval => %d);",
}

var iotCreateHypertableSql = []string{
	"select create_hypertable('air_quality_room','time', chunk_time_interval => %d);",
	"select create_hypertable('air_condition_room','time', chunk_time_interval => %d);",
	"select create_hypertable('air_condition_outdoor','time', chunk_time_interval => %d);",
	"select create_hypertable('camera_detection','time', chunk_time_interval => %d);",
	"select create_hypertable('door_state','time', chunk_time_interval => %d);",
	"select create_hypertable('home_config','time', chunk_time_interval => %d);",
	"select create_hypertable('home_state','time', chunk_time_interval => %d);",
	"select create_hypertable('light_level_room','time', chunk_time_interval => %d);",
	"select create_hypertable('radiator_valve_room','time', chunk_time_interval => %d);",
	"select create_hypertable('water_leakage_room','time', chunk_time_interval => %d);",
	"select create_hypertable('water_level','time', chunk_time_interval => %d);",
	"select create_hypertable('weather_outdoor','time', chunk_time_interval => %d);",
	"select create_hypertable('window_state_room','time', chunk_time_interval => %d);",
}

var devopsCreateIndexSql = []string{
	"CREATE index cpu_hostname_index on cpu(hostname, time DESC);",
	"CREATE index diskio_hostname_index on diskio(hostname, time DESC);",
	"CREATE index disk_hostname_index on disk(hostname, time DESC);",
	"CREATE index kernel_hostname_index on kernel(hostname, time DESC);",
	"CREATE index mem_hostname_index on mem(hostname, time DESC);",
	"CREATE index Net_hostname_index on Net(hostname, time DESC);",
	"CREATE index nginx_hostname_index on nginx(hostname, time DESC);",
	"CREATE index postgresl_hostname_index on postgresl(hostname, time DESC);",
	"CREATE index redis_hostname_index on redis(hostname, time DESC);",
}

var iotCreateIndexSql = []string{
	"CREATE index air_quality_room_home_index on air_quality_room(home_id, time DESC);",
	"CREATE index air_condition_room_home_index on air_condition_room(home_id, time DESC);",
	"CREATE index air_condition_outdoor_home_index on air_condition_outdoor(home_id, time DESC);",
	"CREATE index camera_detection_home_index on camera_detection(home_id, time DESC);",
	"CREATE index door_state_home_index on door_state(home_id, time DESC);",
	"CREATE index home_config_home_index on home_config(home_id, time DESC);",
	"CREATE index home_state_home_index on home_state(home_id, time DESC);",
	"CREATE index light_level_room_home_index on light_level_room(home_id, time DESC);",
	"CREATE index radiator_valve_room_home_index on radiator_valve_room(home_id, time DESC);",
	"CREATE index water_leakage_room_home_index on water_leakage_room(home_id, time DESC);",
	"CREATE index water_level_home_index on water_level(home_id, time DESC);",
	"CREATE index weather_outdoor_home_index on weather_outdoor(home_id, time DESC);",
	"CREATE index window_state_room_home_index on window_state_room(home_id, time DESC);",
}

func (l *TimescaleBulkLoad) createDatabase(daemon_url string) {
	//# Example DSN
	//user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=password", "localhost", uint16(5432), l.psUser)
	fmt.Println("*****", dsn)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}

	_, err = conn.Exec(context.Background(), createDatabaseSql)

	conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	//# Example DSN
	//user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
	dsn = fmt.Sprintf("host=%s port=%d user=%s password=password database=benchmark_db", "localhost", uint16(5432), l.psUser)
	fmt.Println("***** 2", dsn)
	config, err = pgx.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	conn, err = pgx.ConnectConfig(context.Background(), config)

	defer func() {
		conn.Close(context.Background())
	}()
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Exec(context.Background(), createExtensionSql)
	if err != nil {
		log.Fatal(err)
	}
	//TODO create only use-case specific schema
	for _, sql := range DevopsCreateTableSql {
		_, err = conn.Exec(context.Background(), sql)
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range IotCreateTableSql {
		_, err = conn.Exec(context.Background(), sql)
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range devopsCreateIndexSql {
		_, err = conn.Exec(context.Background(), sql)
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range iotCreateIndexSql {
		_, err = conn.Exec(context.Background(), sql)
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range devopsCreateHypertableSql {
		_, err = conn.Exec(context.Background(), fmt.Sprintf(sql, l.chunkDuration.Nanoseconds()))
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, sql := range iotCreateHypertableSql {
		_, err = conn.Exec(context.Background(), fmt.Sprintf(sql, l.chunkDuration.Nanoseconds()))
		fmt.Println(sql)
		if err != nil {
			log.Fatal(err)
		}
	}

}
