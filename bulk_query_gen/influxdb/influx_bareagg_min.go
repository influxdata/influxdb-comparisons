package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

func NewInfluxQLBareAggregateMin(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxBareAggregateQuery(Min, InfluxQL, dbConfig, queriesFullRange, scaleVar)
}

func NewFluxBareAggregateMin(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, _ time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return NewInfluxBareAggregateQuery(Min, Flux, dbConfig, queriesFullRange, scaleVar)
}
