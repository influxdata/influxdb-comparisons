package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxQL query for "Group Window" on the standard cardinality IoT dataset
func NewInfluxQLGroupWindowTransposeLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Last, LowCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux Query query for "Group Window" on the standard cardinality IoT dataset
func NewFluxGroupWindowTransposeLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Last, LowCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// InfluxQL query for "Group Window" on the high cardinality Metaquery dataset
func NewInfluxQLGroupWindowTransposeLastCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Last, HighCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux query for "Group Window" on the high cardinality Metaquery dataset
func NewFluxGroupWindowTransposeLastCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Last, HighCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
