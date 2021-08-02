package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLWindowAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Last, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

func NewFluxWindowAggregateLast(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Last, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
