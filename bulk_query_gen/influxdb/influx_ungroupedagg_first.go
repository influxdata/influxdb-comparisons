package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLUngroupedAggregateFirst(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(First, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxUngroupedAggregateFirst(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(First, Flux, dbConfig, queriesFullRange, scaleVar)
}
