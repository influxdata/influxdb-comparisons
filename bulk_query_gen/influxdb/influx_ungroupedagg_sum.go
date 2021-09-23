package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLUngroupedAggregateSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Sum, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxUngroupedAggregateSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Sum, Flux, dbConfig, queriesFullRange, scaleVar)
}
