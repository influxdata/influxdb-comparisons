package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLWindowAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Count, InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar)
}

func NewFluxWindowAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxWindowAggregateQuery(Count, Flux, dbConfig, queriesFullRange, queryInterval, scaleVar)
}
