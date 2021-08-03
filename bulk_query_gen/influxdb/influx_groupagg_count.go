package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLGroupAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Count, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxGroupAggregateCount(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Count, Flux, dbConfig, queriesFullRange, scaleVar)
}
