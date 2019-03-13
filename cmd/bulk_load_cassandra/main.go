// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"strconv"
	"strings"
)

type CassandraBulkLoad struct {
	// Program option vars:
	daemonUrl    string
	writeTimeout time.Duration
	compressor   string
	useCase      string

	// Global vars
	batchChan    chan *gocql.Batch
	inputDone    chan struct{}
	session      *gocql.Session
	scanFinished bool
}

var load = &CassandraBulkLoad{}

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

func (l *CassandraBulkLoad) Init() {
	flag.StringVar(&l.daemonUrl, "url", "localhost:9042", "Cassandra URL.")

	flag.DurationVar(&l.writeTimeout, "write-timeout", 60*time.Second, "Write timeout.")
	flag.StringVar(&l.compressor, "compressor", "LZ4Compressor", "Table compressor: DeflateCompressor, LZ4Compressor or SnappyCompressor ")
	flag.StringVar(&l.useCase, "use-case", common.UseCaseChoices[0], "Use case to set specific load behavior. Options: "+strings.Join(common.UseCaseChoices, ","))
}

func (l *CassandraBulkLoad) Validate() {

}

func (l *CassandraBulkLoad) CreateDb() {
	var ucTablesMap = map[string][]string{
		common.UseCaseDevOps: createTablesCQLDevops,
		common.UseCaseIot:    createTablesCQLIot,
	}

	log.Println("Creating keyspace")
	if tablesCql, ok := ucTablesMap[l.useCase]; ok {
		l.createKeyspace(l.daemonUrl, tablesCql)
	} else {
		log.Fatalf("Unsupport use-case: %s\n", l.useCase)
	}
}

func (l *CassandraBulkLoad) PrepareWorkers() {
	if bulk_load.Runner.DoLoad {
		cluster := gocql.NewCluster(l.daemonUrl)
		cluster.Keyspace = "measurements"
		cluster.Timeout = l.writeTimeout
		cluster.Consistency = gocql.One
		cluster.ProtoVersion = 4
		var err error
		l.session, err = cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
	}

	l.batchChan = make(chan *gocql.Batch, bulk_load.Runner.Workers)
	l.inputDone = make(chan struct{})
}

func (l *CassandraBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *CassandraBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *CassandraBulkLoad) PrepareProcess(i int) {

}

func (l *CassandraBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	return l.processBatches(l.session, waitGroup)
}

func (l *CassandraBulkLoad) AfterRunProcess(i int) {

}

func (l *CassandraBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *CassandraBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *CassandraBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *CassandraBulkLoad) CleanUp() {
	if l.session != nil {
		l.session.Close()
	}
}

func (l *CassandraBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {

	reportTags = [][2]string{{"write_timeout", strconv.Itoa(int(l.writeTimeout))}}
	params.DBType = "Cassandra"
	params.DestinationUrl = l.daemonUrl

	return
}

// scan reads lines from stdin. It expects input in the Cassandra CQL format.
func (l *CassandraBulkLoad) RunScanner(syncChanDone chan int) (int64, int64, int64) {
	l.scanFinished = false
	var batch *gocql.Batch
	if bulk_load.Runner.DoLoad {
		batch = l.session.NewBatch(gocql.LoggedBatch)
	}

	var n int
	var err error
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}

	scanner := bufio.NewScanner(os.Stdin)
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
		itemsRead++
		bytesRead += int64(len(scanner.Bytes()))

		if !bulk_load.Runner.DoLoad {
			continue
		}

		batch.Query(string(scanner.Bytes()))

		n++
		if n >= bulk_load.Runner.BatchSize {
			l.batchChan <- batch
			batch = l.session.NewBatch(gocql.LoggedBatch)
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
		l.batchChan <- batch
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	//cassandra's schema stores each value separately, point is represented in series_id
	if itemsRead != totalPoints {
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read items: %d, expected: %d:", itemsRead, totalPoints)
		} else {
			totalValues = int64(float64(itemsRead) * bulk_load.ValuesPerMeasurement) // needed for statistics summary
		}
	}
	l.scanFinished = true
	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *CassandraBulkLoad) processBatches(session *gocql.Session, waitGroup *sync.WaitGroup) error {
	var rerr error
	for batch := range l.batchChan {
		if !bulk_load.Runner.DoLoad {
			continue
		}

		// Write the batch.
		err := session.ExecuteBatch(batch)
		if err != nil {
			rerr = fmt.Errorf("Error writing: %s\n", err.Error())
			break
		}
	}
	waitGroup.Done()
	return rerr
}

var tableOptionsFmt = "with compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'} and compression = {'class':'%s'}"

var createTablesCQLDevops = []string{
	"CREATE table measurements.cpu(time bigint,hostname TEXT,region TEXT,datacenter TEXT,rack TEXT,os TEXT,arch TEXT,team TEXT,service TEXT,service_version TEXT,service_environment TEXT,usage_user double,usage_system double,usage_idle double,usage_nice double,usage_iowait double,usage_irq double,usage_softirq double,usage_steal double,usage_guest double,usage_guest_nice double, primary key(hostname, time)) %s;",
	"CREATE table measurements.diskio(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, serial TEXT, reads bigint, writes bigint, read_bytes bigint, write_bytes bigint, read_time bigint, write_time bigint, io_time bigint , primary key(hostname, time)) %s;",
	"CREATE table measurements.disk(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, path TEXT, fstype TEXT, total bigint, free bigint, used bigint, used_percent bigint, inodes_total bigint, inodes_free bigint, inodes_used bigint, primary key(hostname, time)) %s;",
	"CREATE table measurements.kernel(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, boot_time bigint, interrupts bigint, context_switches bigint, processes_forked bigint, disk_pages_in bigint, disk_pages_out bigint, primary key(hostname, time)) %s;",
	"CREATE table measurements.mem(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, total bigint, available bigint, used bigint, free bigint, cached bigint, buffered bigint, used_percent double, available_percent double, buffered_percent double, primary key(hostname, time)) %s;",
	"CREATE table measurements.Net(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, interface TEXT, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint , primary key(hostname, time)) %s;",
	"CREATE table measurements.nginx(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, accepts bigint, active bigint, handled bigint, reading bigint, requests bigint, waiting bigint, writing bigint , primary key(hostname, time)) %s;",
	"CREATE table measurements.postgresl(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, numbackends bigint, xact_commit bigint, xact_rollback bigint, blks_read bigint, blks_hit bigint, tup_returned bigint, tup_fetched bigint, tup_inserted bigint, tup_updated bigint, tup_deleted bigint, conflicts bigint, temp_files bigint, temp_bytes bigint, deadlocks bigint, blk_read_time bigint, blk_write_time bigint , primary key(hostname, time)) %s;",
	"CREATE table measurements.redis(time bigint, hostname TEXT, region TEXT, datacenter TEXT, rack TEXT, os TEXT, arch TEXT, team TEXT, service TEXT, service_version TEXT, service_environment TEXT, port TEXT, server TEXT, uptime_in_seconds bigint, total_connections_received bigint, expired_keys bigint, evicted_keys bigint, keyspace_hits bigint, keyspace_misses bigint, instantaneous_ops_per_sec bigint, instantaneous_input_kbps bigint, instantaneous_output_kbps bigint, connected_clients bigint, used_memory bigint, used_memory_rss bigint, used_memory_peak bigint, used_memory_lua bigint, rdb_changes_since_last_save bigint, sync_full bigint, sync_partial_ok bigint, sync_partial_err bigint, pubsub_channels bigint, pubsub_patterns bigint, latest_fork_usec bigint, connected_slaves bigint, master_repl_offset bigint, repl_backlog_active bigint, repl_backlog_size bigint, repl_backlog_histlen bigint, mem_fragmentation_ratio bigint, used_cpu_sys bigint, used_cpu_user bigint, used_cpu_sys_children bigint, used_cpu_user_children bigint , primary key(hostname, time)) %s;",
}

var createTablesCQLIot = []string{
	"CREATE TABLE measurements.air_quality_room (time bigint,room_id TEXT,sensor_id TEXT,home_id TEXT, co2_level double,co_level double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.air_condition_room (time bigint,room_id TEXT,sensor_id TEXT,home_id TEXT, temperature double,humidity double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.air_condition_outdoor (time bigint,sensor_id TEXT,home_id TEXT, temperature double,humidity double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.camera_detection (time bigint,sensor_id TEXT,home_id TEXT, object_type blob,object_kind blob,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.door_state (time bigint,door_id TEXT,sensor_id TEXT, home_id TEXT, state double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.home_config (time bigint,sensor_id TEXT,home_id TEXT, config_string blob, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.home_state (time bigint,sensor_id TEXT,home_id TEXT, state BIGINT,state_string blob, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.light_level_room (time bigint,room_id TEXT,sensor_id TEXT,home_id TEXT, level double, battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.radiator_valve_room (time bigint,room_id TEXT,radiator TEXT,sensor_id TEXT,home_id TEXT, opening_level double, battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.water_leakage_room (time bigint,sensor_id TEXT,room_id TEXT,home_id TEXT, leakage double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.water_level (time bigint,sensor_id TEXT,home_id TEXT, level double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.weather_outdoor (time bigint,sensor_id TEXT,home_id TEXT, pressure double,wind_speed double,wind_direction double,precipitation double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.window_state_room (time bigint,room_id TEXT,sensor_id TEXT,window_id TEXT,home_id TEXT, state double,battery_voltage double, primary key(home_id, time)) %s;",
}

func (l *CassandraBulkLoad) createKeyspace(daemonUrl string, tableSchema []string) {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Timeout = l.writeTimeout
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	if err := session.Query(`create keyspace measurements with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`).Exec(); err != nil {
		log.Print("if you know what you are doing, drop the keyspace with a command line:")
		log.Print("echo 'drop keyspace measurements;' | cqlsh <host>")
		log.Fatal(err)
	}
	tableOptions := fmt.Sprintf(tableOptionsFmt, l.compressor)

	for _, tableCQLFormat := range tableSchema {
		tableCQL := fmt.Sprintf(tableCQLFormat, tableOptions)
		if err := session.Query(tableCQL).Exec(); err != nil {
			log.Fatal(err)
		}
	}
}
