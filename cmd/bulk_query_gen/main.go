// bulk_query_gen generates queries for various use cases. Its output will
// be consumed by query_benchmarker.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	bulkQueryGen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/cassandra"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/elasticsearch"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/influxdb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/opentsdb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/timescaledb"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"
)

const (
	DevOps                          = "devops"
	DevOpsOneHostOneHour            = "1-host-1-hr"
	DevOpsOneHostTwelveHours        = "1-host-12-hr"
	DevOpsEightHostsOneHour         = "8-host-1-hr"
	DevOpsGroupBy                   = "groupby"
	Iot                             = "iot"
	IotOneHomeTwelveHours           = "1-home-12-hours"
	Dashboard                       = "dashboard"
	DashboardAll                    = "dashboard-all"
	DashboardAvailability           = "availability"
	DashboardCpuNum                 = "cpu-num"
	DashboardCpuUtilization         = "cpu-utilization"
	DashboardDiskAllocated          = "disk-allocated"
	DashboardDiskUsage              = "disk-usage"
	DashboardDiskUtilization        = "disk-utilization"
	DashboardHttpRequestDuration    = "http-request-duration"
	DashboardHttpRequests           = "http-requests"
	DashboardKapaCpu                = "kapa-cpu"
	DashboardKapaLoad               = "kapa-load"
	DashboardKapaRam                = "kapa-ram"
	DashboardMemoryTotal            = "memory-total"
	DashboardMemoryUtilization      = "memory-utilization"
	DashboardNginxRequests          = "nginx-requests"
	DashboardQueueBytes             = "queue-bytes"
	DashboardRedisMemoryUtilization = "redis-memory-utilization"
	DashboardSystemLoad             = "system-load"
	DashboardThroughput             = "throughput"
)

// query generator choices {use-case, query-type, format}
// (This object is shown to the user when flag.Usage is called.)
var useCaseMatrix = map[string]map[string]map[string]bulkQueryGen.QueryGeneratorMaker{
	DevOps: {
		DevOpsOneHostOneHour: {
			"cassandra":        cassandra.NewCassandraDevopsSingleHost,
			"es-http":          elasticsearch.NewElasticSearchDevopsSingleHost,
			"influx-flux-http": influxdb.NewFluxDevopsSingleHost,
			"influx-http":      influxdb.NewInfluxQLDevopsSingleHost,
			"mongo":            mongodb.NewMongoDevopsSingleHost,
			"opentsdb":         opentsdb.NewOpenTSDBDevopsSingleHost,
			"timescaledb":      timescaledb.NewTimescaleDevopsSingleHost,
		},
		DevOpsOneHostTwelveHours: {
			"cassandra":        cassandra.NewCassandraDevopsSingleHost12hr,
			"es-http":          elasticsearch.NewElasticSearchDevopsSingleHost12hr,
			"influx-flux-http": influxdb.NewFluxDevopsSingleHost12hr,
			"influx-http":      influxdb.NewInfluxQLDevopsSingleHost12hr,
			"mongo":            mongodb.NewMongoDevopsSingleHost12hr,
			"opentsdb":         opentsdb.NewOpenTSDBDevopsSingleHost12hr,
			"timescaledb":      timescaledb.NewTimescaleDevopsSingleHost12hr,
		},
		DevOpsEightHostsOneHour: {
			"cassandra":        cassandra.NewCassandraDevops8Hosts,
			"es-http":          elasticsearch.NewElasticSearchDevops8Hosts,
			"influx-flux-http": influxdb.NewFluxDevops8Hosts,
			"influx-http":      influxdb.NewInfluxQLDevops8Hosts,
			"mongo":            mongodb.NewMongoDevops8Hosts1Hr,
			"opentsdb":         opentsdb.NewOpenTSDBDevops8Hosts,
			"timescaledb":      timescaledb.NewTimescaleDevops8Hosts1Hr,
		},
		DevOpsGroupBy: {
			"cassandra":        cassandra.NewCassandraDevopsGroupBy,
			"es-http":          elasticsearch.NewElasticSearchDevopsGroupBy,
			"influx-flux-http": influxdb.NewFluxDevopsGroupBy,
			"influx-http":      influxdb.NewInfluxQLDevopsGroupBy,
			"timescaledb":      timescaledb.NewTimescaleDevopsGroupby,
		},
	},
	Iot: {
		IotOneHomeTwelveHours: {
			"influx-flux-http": influxdb.NewFluxIotSingleHost,
			"influx-http":      influxdb.NewInfluxQLIotSingleHost,
			"timescaledb":      timescaledb.NewTimescaleIotSingleHost,
			"cassandra":        cassandra.NewCassandraIotSingleHost,
			"mongo":            mongodb.NewMongoIotSingleHost,
		},
	},
	Dashboard: {
		DashboardAll: {
			"influx-http": influxdb.NewInfluxQLDashboardAll,
		},
		DashboardCpuNum: {
			"influx-http": influxdb.NewInfluxQLDashboardCpuNum,
		},
		DashboardAvailability: {
			"influx-http": influxdb.NewInfluxQLDashboardAvailability,
		},
		DashboardCpuUtilization:         {"influx-http": influxdb.NewInfluxQLDashboardCpuUtilization},
		DashboardDiskAllocated:          {"influx-http": influxdb.NewInfluxQLDashboardDiskAllocated},
		DashboardDiskUsage:              {"influx-http": influxdb.NewInfluxQLDashboardDiskUsage},
		DashboardDiskUtilization:        {"influx-http": influxdb.NewInfluxQLDashboardDiskUtilization},
		DashboardHttpRequestDuration:    {"influx-http": influxdb.NewInfluxQLDashboardHttpRequestDuration},
		DashboardHttpRequests:           {"influx-http": influxdb.NewInfluxQLDashboardHttpRequests},
		DashboardKapaCpu:                {"influx-http": influxdb.NewInfluxQLDashboardKapaCpu},
		DashboardKapaLoad:               {"influx-http": influxdb.NewInfluxQLDashboardKapaLoad},
		DashboardKapaRam:                {"influx-http": influxdb.NewInfluxQLDashboardKapaRam},
		DashboardMemoryTotal:            {"influx-http": influxdb.NewInfluxQLDashboardMemoryTotal},
		DashboardMemoryUtilization:      {"influx-http": influxdb.NewInfluxQLDashboardMemoryUtilization},
		DashboardNginxRequests:          {"influx-http": influxdb.NewInfluxQLDashboardNginxRequests},
		DashboardQueueBytes:             {"influx-http": influxdb.NewInfluxQLDashboardQueueBytes},
		DashboardRedisMemoryUtilization: {"influx-http": influxdb.NewInfluxQLDashboardRedisMemoryUtilization},
		DashboardSystemLoad:             {"influx-http": influxdb.NewInfluxQLDashboardSystemLoad},
		DashboardThroughput:             {"influx-http": influxdb.NewInfluxQLDashboardThroughput},
	},
}

// Program option vars:
var (
	useCase   string
	queryType string
	format    string

	scaleVar   int
	queryCount int

	dbName string // TODO(rw): make this a map[string]string -> DatabaseConfig

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time
	queryInterval  time.Duration
	timeWindowShift time.Duration

	seed  int64
	debug int

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint
)

// Parse args:
func init() {
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := flag.Usage
	flag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt, formats := range queryTypes {
				for f := range formats {
					fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s, format: %s\n", uc, qt, f)
				}
			}
		}
	}

	flag.StringVar(&format, "format", "influx-http", "Format to emit. (Choices are in the use case matrix.)")
	flag.StringVar(&useCase, "use-case", "devops", "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scalevar used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database for influx to use (ignored for ElasticSearch).")

	flag.StringVar(&timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	flag.DurationVar(&queryInterval, "query-interval", bulkQueryGen.DefaultQueryInterval, "Time interval query should ask for.")
	flag.DurationVar(&timeWindowShift, "time-window-shift", -1, "Sliding time window shift. (When set to > 0s, queries option is ignored - number of queries is calculated.")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1) (default 0).")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.Parse()

	if queryType == DevOpsEightHostsOneHour && scaleVar < 8 {
		log.Fatal("\"scale-var\" must be greater than the hosts grouping number")
	}

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	if _, ok := useCaseMatrix[useCase]; !ok {
		log.Fatal("invalid use case specifier")
	}

	if _, ok := useCaseMatrix[useCase][queryType]; !ok {
		log.Fatal("invalid query type specifier")
	}

	if _, ok := useCaseMatrix[useCase][queryType][format]; !ok {
		log.Fatal("invalid format specifier")
	}

	hourGroupInterval := 1

	if queryType == DevOpsOneHostTwelveHours {
		hourGroupInterval = 12
	}

	// Parse timestamps:
	var err error
	timestampStart, err = time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()

	duration := timestampEnd.Sub(timestampStart)

	if duration.Nanoseconds() < 0 {
		log.Fatal("\"timestamp-end\" must be grater than \"timestamp-start\"")
	}

	if duration.Nanoseconds()/time.Hour.Nanoseconds() < int64(hourGroupInterval) {
		log.Fatal("Time interval must be greater than the grouping interval")
	}
	if duration.Nanoseconds() < queryInterval.Nanoseconds() {
		log.Fatal("Query interval must be greater than the grouping interval")
	}

	// TODO temporary for benchmarks
	if useCase == Dashboard && timeWindowShift == -1 { // when not set for dashboard, always use 5s default
		timeWindowShift = 5 * time.Second
	}

	if timeWindowShift > 0 {
		bulkQueryGen.TimeWindowShift = timeWindowShift // global
		queryCount = int(timestampEnd.Sub(timestampStart).Seconds() / timeWindowShift.Seconds())
		if (queryType == DashboardAll) {
			queryCount *= 18
		}
		log.Printf("%v queries will be generated to cover time interval using %v shift", queryCount,  timeWindowShift)
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)
}

func main() {
	rand.Seed(seed)

	dbConfig := bulkQueryGen.DatabaseConfig{
		bulkQueryGen.DatabaseName: dbName,
	}

	// Make the query generator:
	maker := useCaseMatrix[useCase][queryType][format]
	interval := bulkQueryGen.NewTimeInterval(timestampStart, timestampEnd)
	var generator = maker(dbConfig, interval, queryInterval, scaleVar)

	// Set up bookkeeping:
	stats := make(map[string]int64)

	// Set up output buffering:
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	// Create request instances, serializing them to stdout and collecting
	// counts for each kind. If applicable, only prints queries that
	// belong to this interleaved group id:
	var currentInterleavedGroup uint = 0

	enc := gob.NewEncoder(out)
	for i := 0; i < queryCount; i++ {
		q := generator.Dispatch(i)

		if currentInterleavedGroup == interleavedGenerationGroupID {
			err := enc.Encode(q)
			if err != nil {
				log.Fatal("encoder ", err)
			}
			stats[string(q.HumanLabelName())]++

			if debug == 1 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanLabelName())
				if err != nil {
					log.Fatal(err)
				}
			} else if debug == 2 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.HumanDescriptionName())
				if err != nil {
					log.Fatal(err)
				}
			} else if debug >= 3 {
				_, err := fmt.Fprintf(os.Stderr, "%s\n", q.String())
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		q.Release()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}

	// Print stats:
	keys := []string{}
	for k, _ := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, err := fmt.Fprintf(os.Stderr, "%s: %d points\n", k, stats[k])
		if err != nil {
			log.Fatal(err)
		}
	}
}
