package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxQL query for "Group Window" on the standard cardinality IoT dataset
func NewInfluxQLGroupWindowTransposeCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Count, LowCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux Query query for "Group Window" on the standard cardinality IoT dataset
func NewFluxGroupWindowTransposeCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Count, LowCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// InfluxQL query for "Group Window" on the high cardinality Metaquery dataset
func NewInfluxQLGroupWindowTransposeCountCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Count, HighCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux query for "Group Window" on the high cardinality Metaquery dataset
func NewFluxGroupWindowTransposeCountCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Count, HighCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
