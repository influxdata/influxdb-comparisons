package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLGroupAggregateMax(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Max, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxGroupAggregateMax(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxGroupAggregateQuery(Max, Flux, dbConfig, queriesFullRange, scaleVar)
}
