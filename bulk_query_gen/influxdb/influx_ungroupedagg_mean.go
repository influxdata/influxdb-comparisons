package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLUngroupedAggregateMean(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Mean, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxUngroupedAggregateMean(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxUngroupedAggregateQuery(Mean, Flux, dbConfig, queriesFullRange, scaleVar)
}
