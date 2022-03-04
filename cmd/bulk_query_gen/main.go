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
	"strings"
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
	IotAggregateKeep                = "aggregate-keep"
	IotAggregateDrop                = "aggregate-drop"
	IotSortedPivot                  = "sorted-pivot"
	IotFastQuerySmallData           = "fast-query-small-data"
	IotStandAloneFilter             = "standalone-filter"
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
	MetaqueryCardinality            = "cardinality"
	MultiMeasurementOr              = "multi-measurement-or"
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
		IotFastQuerySmallData: { // alias for IotOneHomeTwelveHours
			"influx-flux-http": influxdb.NewFluxIotSingleHost,
			"influx-http":      influxdb.NewInfluxQLIotSingleHost,
		},
		IotAggregateKeep: {
			"influx-flux-http": influxdb.NewFluxIotAggregateKeep,
			"influx-http":      influxdb.NewInfluxQLIotAggregateKeep,
		},
		IotAggregateDrop: {
			"influx-flux-http": influxdb.NewFluxIotAggregateDrop,
			"influx-http":      influxdb.NewInfluxQLIotAggregateDrop,
		},
		IotStandAloneFilter: {
			"influx-flux-http": influxdb.NewFluxIotStandAloneFilter,
			"influx-http":      influxdb.NewInfluxQLIotStandAloneFilter,
		},
		IotSortedPivot: {
			"influx-flux-http": influxdb.NewFluxIotSortedPivot,
			"influx-http":      influxdb.NewInfluxQLIotSortedPivot,
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
		MetaqueryCardinality: {
			"influx-flux-http": influxdb.NewFluxMetaqueryCardinality,
			"influx-http":      influxdb.NewInfluxQLMetaqueryCardinality,
		},
	},
	common.UseCaseWindowAggregate: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateCount,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateCount,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateSum,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateSum,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateMean,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateMean,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateMin,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateMin,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateMax,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateMax,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateFirst,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateFirst,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxWindowAggregateLast,
			"influx-http":      influxdb.NewInfluxQLWindowAggregateLast,
		},
	},
	common.UseCaseGroupAggregate: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateCount,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateCount,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateSum,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateSum,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateMean,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateMean,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateFirst,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateFirst,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateLast,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateLast,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateMin,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateMin,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxGroupAggregateMax,
			"influx-http":      influxdb.NewInfluxQLGroupAggregateMax,
		},
	},
	common.UseCaseBareAggregate: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxBareAggregateCount,
			"influx-http":      influxdb.NewInfluxQLBareAggregateCount,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxBareAggregateSum,
			"influx-http":      influxdb.NewInfluxQLBareAggregateSum,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxBareAggregateMean,
			"influx-http":      influxdb.NewInfluxQLBareAggregateMean,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxBareAggregateFirst,
			"influx-http":      influxdb.NewInfluxQLBareAggregateFirst,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxBareAggregateLast,
			"influx-http":      influxdb.NewInfluxQLBareAggregateLast,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxBareAggregateMin,
			"influx-http":      influxdb.NewInfluxQLBareAggregateMin,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxBareAggregateMax,
			"influx-http":      influxdb.NewInfluxQLBareAggregateMax,
		},
	},
	common.UseCaseUngroupedAggregate: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateCount,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateCount,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateSum,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateSum,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateMean,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateMean,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateFirst,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateFirst,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateLast,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateLast,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateMin,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateMin,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxUngroupedAggregateMax,
			"influx-http":      influxdb.NewInfluxQLUngroupedAggregateMax,
		},
	},
	common.UseCaseGroupWindowTransposeHighCard: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeCountCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeCountCardinality,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeSumCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeSumCardinality,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMeanCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMeanCardinality,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeFirstCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeFirstCardinality,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeLastCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeLastCardinality,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMinCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMinCardinality,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMaxCardinality,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMaxCardinality,
		},
	},
	common.UseCaseGroupWindowTransposeLowCard: {
		string(influxdb.Count): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeCount,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeCount,
		},
		string(influxdb.Sum): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeSum,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeSum,
		},
		string(influxdb.Mean): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMean,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMean,
		},
		string(influxdb.First): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeFirst,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeFirst,
		},
		string(influxdb.Last): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeLast,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeLast,
		},
		string(influxdb.Min): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMin,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMin,
		},
		string(influxdb.Max): {
			"influx-flux-http": influxdb.NewFluxGroupWindowTransposeMax,
			"influx-http":      influxdb.NewInfluxQLGroupWindowTransposeMax,
		},
	},
	common.UseCaseMultiMeasurement: {
		MultiMeasurementOr: {
			"influx-flux-http": influxdb.NewFluxMultiMeasurementOr,
			"influx-http":      influxdb.NewInfluxQLMultiMeasurementOr,
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
	flag.StringVar(&documentFormat, "document-format", "", "Document format database-specific flags. (MongoDB: 'key-pair', 'flat', 'timeseries' - default)")
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
		if documentFormat == "" {
			documentFormat = "timeseries"
		}
		if strings.Contains(documentFormat, mongodb.FlatFormat) {
			mongodb.DocumentFormat = mongodb.FlatFormat
		} else {
			mongodb.DocumentFormat = mongodb.KeyPairFormat
		}
		mongodb.UseTimeseries = strings.Contains(documentFormat, mongodb.TimeseriesFormat)
		if mongodb.UseTimeseries {
			log.Print("Using MongoDB 5+ time series collection")
			mongodb.DocumentFormat = mongodb.FlatFormat
		}
		log.Printf("Using %s point serialization", mongodb.DocumentFormat)
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
