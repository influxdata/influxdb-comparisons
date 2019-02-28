// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
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
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"strconv"
	"strings"
)

// Program option vars:
var (
	daemonUrl      string
	workers        int
	batchSize      int
	doLoad         bool
	writeTimeout   time.Duration
	reportDatabase string
	reportHost     string
	reportUser     string
	reportPassword string
	reportTagsCSV  string
	compressor     string
	useCase        string
)

// Global vars
var (
	batchChan      chan *gocql.Batch
	inputDone      chan struct{}
	workersGroup   sync.WaitGroup
	reportTags     [][2]string
	reportHostname string
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:9042", "Cassandra URL.")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.DurationVar(&writeTimeout, "write-timeout", 60*time.Second, "Write timeout.")
	flag.StringVar(&compressor, "compressor", "LZ4Compressor", "Table compressor: DeflateCompressor, LZ4Compressor or SnappyCompressor ")
	flag.StringVar(&useCase, "use-case", common.UseCaseChoices[0], "Use case to set specific load behavior. Options: "+strings.Join(common.UseCaseChoices, ","))

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.StringVar(&reportDatabase, "report-database", "database_benchmarks", "Database name where to store result metrics")
	flag.StringVar(&reportHost, "report-host", "", "Host to send result metrics")
	flag.StringVar(&reportUser, "report-user", "", "User for host to send result metrics")
	flag.StringVar(&reportPassword, "report-password", "", "User password for Host to send result metrics")
	flag.StringVar(&reportTagsCSV, "report-tags", "", "Comma separated k:v tags to send  alongside result metrics")

	flag.Parse()

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

func main() {
	var ucTablesMap = map[string][]string{
		common.UseCaseDevOps: createTablesCQLDevops,
		common.UseCaseIot:    createTablesCQLIot,
	}

	if doLoad {
		log.Println("Creating keyspace")
		if tablesCql, ok := ucTablesMap[useCase]; ok {
			createKeyspace(daemonUrl, tablesCql)
		} else {
			log.Fatalf("Unsupport use-case: %s\n", useCase)
		}
	}

	var session *gocql.Session

	if doLoad {
		cluster := gocql.NewCluster(daemonUrl)
		cluster.Keyspace = "measurements"
		cluster.Timeout = writeTimeout
		cluster.Consistency = gocql.One
		cluster.ProtoVersion = 4
		var err error
		session, err = cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}
		defer session.Close()
	}

	batchChan = make(chan *gocql.Batch, workers)
	inputDone = make(chan struct{})
	log.Println("Starting workers")
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(session)
	}

	start := time.Now()
	itemsRead, bytesRead, valuesRead := scan(session, batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)

	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())
	valuesRate := float64(valuesRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %.2f/s, mean value rate %.2f/s, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, valuesRate, bytesRate/(1<<20))

	if reportHost != "" {
		//append db specific tags to custom tags
		reportTags = append(reportTags, [2]string{"write_timeout", strconv.Itoa(int(writeTimeout))})

		reportParams := &report.LoadReportParams{
			ReportParams: report.ReportParams{
				DBType:             "Cassandra",
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
		err := report.ReportLoadResult(reportParams, itemsRead, valuesRate, bytesRate, took)

		if err != nil {
			log.Fatal(err)
		}
	}
}

// scan reads lines from stdin. It expects input in the Cassandra CQL format.
func scan(session *gocql.Session, itemsPerBatch int) (int64, int64, int64) {
	var batch *gocql.Batch
	if doLoad {
		batch = session.NewBatch(gocql.LoggedBatch)
	}

	var n int
	var err error
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	scanner := bufio.NewScanner(os.Stdin)
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

		if !doLoad {
			continue
		}

		batch.Query(string(scanner.Bytes()))

		n++
		if n >= itemsPerBatch {
			batchChan <- batch
			batch = session.NewBatch(gocql.LoggedBatch)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- batch
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)
	//cassandra's schema stores each value separately, point is represented in series_id
	if itemsRead != totalPoints {
		log.Fatalf("Incorrent number of read items: %d, expected: %d:", itemsRead, totalPoints)
	}

	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(session *gocql.Session) {
	for batch := range batchChan {
		if !doLoad {
			continue
		}

		// Write the batch.
		err := session.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	workersGroup.Done()
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
	"CREATE TABLE measurements.camera_detection (time bigint,sensor_id TEXT,home_id TEXT, object_type TEXT,object_kind TEXT,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.door_state (time bigint,door_id TEXT,sensor_id TEXT, home_id TEXT, state double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.home_config (time bigint,sensor_id TEXT,home_id TEXT, config_string TEXT, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.home_state (time bigint,sensor_id TEXT,home_id TEXT, state BIGINT,state_string TEXT, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.light_level_room (time bigint,room_id TEXT,sensor_id TEXT,home_id TEXT, level double, battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.radiator_valve_room (time bigint,room_id TEXT,radiator TEXT,sensor_id TEXT,home_id TEXT, opening_level double, battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.water_leakage_room (time bigint,sensor_id TEXT,room_id TEXT,home_id TEXT, leakage double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.water_level (time bigint,sensor_id TEXT,home_id TEXT, level double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.weather_outdoor (time bigint,sensor_id TEXT,home_id TEXT, pressure double,wind_speed double,wind_direction double,precipitation double,battery_voltage double, primary key(home_id, time)) %s;",
	"CREATE TABLE measurements.window_state_room (time bigint,room_id TEXT,sensor_id TEXT,window_id TEXT,home_id TEXT, state double,battery_voltage double, primary key(home_id, time)) %s;",
}

func createKeyspace(daemonUrl string, tableSchema []string) {
	cluster := gocql.NewCluster(daemonUrl)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Timeout = writeTimeout
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
	tableOptions := fmt.Sprintf(tableOptionsFmt, compressor)

	for _, tableCQLFormat := range tableSchema {
		tableCQL := fmt.Sprintf(tableCQLFormat, tableOptions)
		if err := session.Query(tableCQL).Exec(); err != nil {
			log.Fatal(err)
		}
	}
}
