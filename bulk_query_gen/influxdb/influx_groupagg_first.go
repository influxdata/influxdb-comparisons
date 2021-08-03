package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLGroupAggregateFirst(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(First, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxGroupAggregateFirst(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(First, Flux, dbConfig, queriesFullRange, scaleVar)
}
