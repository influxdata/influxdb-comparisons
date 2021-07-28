// bulk_query_gen generates queries for various use cases. Its output will
// be consumed by query_benchmarker.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	bulkQueryGen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/cassandra"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/elasticsearch"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/graphite"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/influxdb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/mongodb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/opentsdb"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/splunk"
	"github.com/influxdata/influxdb-comparisons/bulk_query_gen/timescaledb"
)

const (
	DevOpsOneHostOneHour            = "1-host-1-hr"
	DevOpsOneHostTwelveHours        = "1-host-12-hr"
	DevOpsEightHostsOneHour         = "8-host-1-hr"
	DevOpsGroupBy                   = "groupby"
	IotOneHomeTwelveHours           = "1-home-12-hours"
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
	MetaqueryTagValues              = "tag-values"
	MetaqueryFieldKeys              = "field-keys"
	MetaqueryAggregateKeep          = "aggregate-keep"
)

// query generator choices {use-case, query-type, format}
// (This object is shown to the user when flag.Usage is called.)
var useCaseMatrix = map[string]map[string]map[string]bulkQueryGen.QueryGeneratorMaker{
	common.UseCaseDevOps: {
		DevOpsOneHostOneHour: {
			"cassandra":        cassandra.NewCassandraDevopsSingleHost,
			"es-http":          elasticsearch.NewElasticSearchDevopsSingleHost,
			"influx-flux-http": influxdb.NewFluxDevopsSingleHost,
			"influx-http":      influxdb.NewInfluxQLDevopsSingleHost,
			"mongo":            mongodb.NewMongoDevopsSingleHost,
			"opentsdb":         opentsdb.NewOpenTSDBDevopsSingleHost,
			"timescaledb":      timescaledb.NewTimescaleDevopsSingleHost,
			"graphite":         graphite.NewGraphiteDevopsSingleHost,
			"splunk":           splunk.NewSplunkDevopsSingleHost,
		},
		DevOpsOneHostTwelveHours: {
			"cassandra":        cassandra.NewCassandraDevopsSingleHost12hr,
			"es-http":          elasticsearch.NewElasticSearchDevopsSingleHost12hr,
			"influx-flux-http": influxdb.NewFluxDevopsSingleHost12hr,
			"influx-http":      influxdb.NewInfluxQLDevopsSingleHost12hr,
			"mongo":            mongodb.NewMongoDevopsSingleHost12hr,
			"opentsdb":         opentsdb.NewOpenTSDBDevopsSingleHost12hr,
			"timescaledb":      timescaledb.NewTimescaleDevopsSingleHost12hr,
			"graphite":         graphite.NewGraphiteDevopsSingleHost12hr,
			"splunk":           splunk.NewSplunkDevopsSingleHost12hr,
		},
		DevOpsEightHostsOneHour: {
			"cassandra":        cassandra.NewCassandraDevops8Hosts,
			"es-http":          elasticsearch.NewElasticSearchDevops8Hosts,
			"influx-flux-http": influxdb.NewFluxDevops8Hosts,
			"influx-http":      influxdb.NewInfluxQLDevops8Hosts,
			"mongo":            mongodb.NewMongoDevops8Hosts1Hr,
			"opentsdb":         opentsdb.NewOpenTSDBDevops8Hosts,
			"timescaledb":      timescaledb.NewTimescaleDevops8Hosts1Hr,
			"graphite":         graphite.NewGraphiteDevops8Hosts,
			"splunk":           splunk.NewSplunkDevops8Hosts,
		},
		DevOpsGroupBy: {
			"cassandra":        cassandra.NewCassandraDevopsGroupBy,
			"es-http":          elasticsearch.NewElasticSearchDevopsGroupBy,
			"influx-flux-http": influxdb.NewFluxDevopsGroupBy,
			"influx-http":      influxdb.NewInfluxQLDevopsGroupBy,
			"timescaledb":      timescaledb.NewTimescaleDevopsGroupby,
			"graphite":         graphite.NewGraphiteDevopsGroupBy,
			"splunk":           splunk.NewSplunkDevopsGroupBy,
		},
	},
	common.UseCaseIot: {
		IotOneHomeTwelveHours: {
			"influx-flux-http": influxdb.NewFluxIotSingleHost,
			"influx-http":      influxdb.NewInfluxQLIotSingleHost,
			"timescaledb":      timescaledb.NewTimescaleIotSingleHost,
			"cassandra":        cassandra.NewCassandraIotSingleHost,
			"mongo":            mongodb.NewMongoIotSingleHost,
		},
	},
	common.UseCaseDashboard: {
		DashboardAll: {
			"influx-flux-http": influxdb.NewFluxDashboardAll,
			"influx-http":      influxdb.NewInfluxQLDashboardAll,
		},
		DashboardCpuNum: {
			"influx-flux-http": influxdb.NewFluxDashboardCpuNum,
			"influx-http":      influxdb.NewInfluxQLDashboardCpuNum,
		},
		DashboardAvailability: {
			"influx-flux-http": influxdb.NewFluxDashboardAvailability,
			"influx-http":      influxdb.NewInfluxQLDashboardAvailability,
		},
		DashboardCpuUtilization: {
			"influx-flux-http": influxdb.NewFluxDashboardCpuUtilization,
			"influx-http":      influxdb.NewInfluxQLDashboardCpuUtilization,
		},
		DashboardDiskAllocated: {
			"influx-flux-http": influxdb.NewFluxDashboardDiskAllocated,
			"influx-http":      influxdb.NewInfluxQLDashboardDiskAllocated,
		},
		DashboardDiskUsage: {
			"influx-flux-http": influxdb.NewFluxDashboardDiskUsage,
			"influx-http":      influxdb.NewInfluxQLDashboardDiskUsage,
		},
		DashboardDiskUtilization: {
			"influx-flux-http": influxdb.NewFluxDashboardDiskUtilization,
			"influx-http":      influxdb.NewInfluxQLDashboardDiskUtilization,
		},
		DashboardHttpRequestDuration: {
			"influx-flux-http": influxdb.NewFluxDashboardHttpRequestDuration,
			"influx-http":      influxdb.NewInfluxQLDashboardHttpRequestDuration,
		},
		DashboardHttpRequests: {
			"influx-flux-http": influxdb.NewFluxDashboardHttpRequests,
			"influx-http":      influxdb.NewInfluxQLDashboardHttpRequests,
		},
		DashboardKapaCpu: {
			"influx-flux-http": influxdb.NewFluxDashboardKapaCpu,
			"influx-http":      influxdb.NewInfluxQLDashboardKapaCpu,
		},
		DashboardKapaLoad: {
			"influx-flux-http": influxdb.NewFluxDashboardKapaLoad,
			"influx-http":      influxdb.NewInfluxQLDashboardKapaLoad,
		},
		DashboardKapaRam: {
			"influx-flux-http": influxdb.NewFluxDashboardKapaRam,
			"influx-http":      influxdb.NewInfluxQLDashboardKapaRam,
		},
		DashboardMemoryTotal: {
			"influx-flux-http": influxdb.NewFluxDashboardMemoryTotal,
			"influx-http":      influxdb.NewInfluxQLDashboardMemoryTotal,
		},
		DashboardMemoryUtilization: {
			"influx-flux-http": influxdb.NewFluxDashboardMemoryUtilization,
			"influx-http":      influxdb.NewInfluxQLDashboardMemoryUtilization,
		},
		DashboardNginxRequests: {
			"influx-flux-http": influxdb.NewFluxDashboardNginxRequests,
			"influx-http":      influxdb.NewInfluxQLDashboardNginxRequests,
		},
		DashboardQueueBytes: {
			"influx-flux-http": influxdb.NewFluxDashboardQueueBytes,
			"influx-http":      influxdb.NewInfluxQLDashboardQueueBytes,
		},
		DashboardRedisMemoryUtilization: {
			"influx-flux-http": influxdb.NewFluxDashboardRedisMemoryUtilization,
			"influx-http":      influxdb.NewInfluxQLDashboardRedisMemoryUtilization,
		},
		DashboardSystemLoad: {
			"influx-flux-http": influxdb.NewFluxDashboardSystemLoad,
			"influx-http":      influxdb.NewInfluxQLDashboardSystemLoad,
		},
		DashboardThroughput: {
			"influx-flux-http": influxdb.NewFluxDashboardThroughput,
			"influx-http":      influxdb.NewInfluxQLDashboardThroughput,
		},
	},
	common.UseCaseMetaquery: {
		MetaqueryTagValues: {
			"influx-flux-http": influxdb.NewFluxMetaqueryTagValues,
			"influx-http":      influxdb.NewInfluxQLMetaqueryTagValues,
		},
		MetaqueryFieldKeys: {
			"influx-flux-http": influxdb.NewFluxMetaqueryFieldKeys,
			"influx-http":      influxdb.NewInfluxQLMetaqueryFieldKeys,
		},
		MetaqueryAggregateKeep: {
			"influx-flux-http": influxdb.NewFluxMetaqueryAggregateKeep,
			"influx-http":      influxdb.NewInfluxQLMetaqueryAggregateKeep,
		},
	},
}

// Program option vars:
var (
	useCase        string
	queryType      string
	format         string
	documentFormat string

	scaleVar   int
	queryCount int

	dbName string // TODO(rw): make this a map[string]string -> DatabaseConfig

	timestampStartStr string
	timestampEndStr   string

	timestampStart    time.Time
	timestampEnd      time.Time
	queryInterval     time.Duration
	timeWindowShift   time.Duration
	queryIntervalType string

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
	flag.StringVar(&documentFormat, "document-format", "", "Document format specification. (for MongoDB format 'simpleArrays'; leave empty for previous behaviour)")
	flag.StringVar(&useCase, "use-case", common.UseCaseChoices[0], "Use case to model. (Choices are in the use case matrix.)")
	flag.StringVar(&queryType, "query-type", "", "Query type. (Choices are in the use case matrix.)")

	flag.IntVar(&scaleVar, "scale-var", 1, "Scaling variable (must be the equal to the scale-var used for data generation).")
	flag.IntVar(&queryCount, "queries", 1000, "Number of queries to generate.")
	flag.StringVar(&dbName, "db", "benchmark_db", "Database to use (ignored for ElasticSearch).")

	flag.StringVar(&timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")
	flag.DurationVar(&queryInterval, "query-interval", bulkQueryGen.DefaultQueryInterval, "Time interval query should ask for.")
	flag.StringVar(&queryIntervalType, "query-interval-type", "window", "Interval type query { window - either random or shifted, last - interval is defined relative to now() }")
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

	// The grouping interval is not applicable for the metaquery benchmarks.
	if useCase != common.UseCaseMetaquery {
		if duration.Nanoseconds()/time.Hour.Nanoseconds() < int64(hourGroupInterval) {
			log.Fatal("Time interval must be greater than the grouping interval")
		}
		if duration.Nanoseconds() < queryInterval.Nanoseconds() {
			log.Fatal("Query interval must be greater than the grouping interval")
		}
	}

	bulkQueryGen.QueryIntervalType = queryIntervalType
	switch queryIntervalType {
	case "window":
		if useCase == common.UseCaseDashboard && timeWindowShift <= 0 { // when not set, always use 5s default for dashboard
			timeWindowShift = 5 * time.Second
		}
	case "last":
		timeWindowShift = 0
	case "recent":
		if queryInterval.Hours() <= 24 {
			log.Fatalf("Query interval type '%s' can only be used with query interval longer than 24h\n", queryIntervalType)
		}
		timeWindowShift = 0
	default:
		log.Fatalf("Unsupported query interval type: %s\n", queryIntervalType)
	}

	if timeWindowShift > 0 {
		bulkQueryGen.TimeWindowShift = timeWindowShift // global
		queryCount = int(timestampEnd.Sub(timestampStart).Seconds() / timeWindowShift.Seconds())
		if queryType == DashboardAll {
			queryCount *= 18
		}
		log.Printf("%v queries will be generated to cover time interval using %v shift", queryCount, timeWindowShift)
	}

	if format == "mongo" {
		if documentFormat != mongodb.SimpleArraysFormat {
			documentFormat = "default"
		}
		mongodb.DocumentFormat = documentFormat
		log.Printf("Using '%s' mongo serialization", mongodb.DocumentFormat)
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
