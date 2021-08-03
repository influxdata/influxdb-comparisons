package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLBareAggregateSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxBareAggregateQuery(Sum, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxBareAggregateSum(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxBareAggregateQuery(Sum, Flux, dbConfig, queriesFullRange, scaleVar)
}
