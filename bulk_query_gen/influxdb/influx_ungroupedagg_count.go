package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLUngroupedAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Count, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxUngroupedAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Count, Flux, dbConfig, queriesFullRange, scaleVar)
}
