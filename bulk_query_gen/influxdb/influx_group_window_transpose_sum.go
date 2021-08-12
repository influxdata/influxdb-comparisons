package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxQL query for "Group Window" on the standard cardinality IoT dataset
func NewInfluxQLGroupWindowTransposeSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Sum, LowCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux Query query for "Group Window" on the standard cardinality IoT dataset
func NewFluxGroupWindowTransposeSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Sum, LowCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// InfluxQL query for "Group Window" on the high cardinality Metaquery dataset
func NewInfluxQLGroupWindowTransposeSumCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Sum, HighCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux query for "Group Window" on the high cardinality Metaquery dataset
func NewFluxGroupWindowTransposeSumCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Sum, HighCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
