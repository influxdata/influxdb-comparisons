package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxQL query for "Group Window" on the standard cardinality IoT dataset
func NewInfluxQLGroupWindowTransposeMax(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Max, LowCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux Query query for "Group Window" on the standard cardinality IoT dataset
func NewFluxGroupWindowTransposeMax(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Max, LowCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// InfluxQL query for "Group Window" on the high cardinality Metaquery dataset
func NewInfluxQLGroupWindowTransposeMaxCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Max, HighCardinality, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

// Flux query for "Group Window" on the high cardinality Metaquery dataset
func NewFluxGroupWindowTransposeMaxCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupWindowTransposeQuery(Max, HighCardinality, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
