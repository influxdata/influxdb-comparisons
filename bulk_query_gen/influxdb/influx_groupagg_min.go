package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLGroupAggregateMin(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Min, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxGroupAggregateMin(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Min, Flux, dbConfig, queriesFullRange, scaleVar)
}
