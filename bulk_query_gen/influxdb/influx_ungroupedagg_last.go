package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLUngroupedAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Last, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxUngroupedAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Last, Flux, dbConfig, queriesFullRange, scaleVar)
}
