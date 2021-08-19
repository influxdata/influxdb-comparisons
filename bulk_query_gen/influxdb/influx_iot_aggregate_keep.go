package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxIotAggregateKeep produces queries that will test performance
// on Flux statements with keep() |> aggregateWindow()
type InfluxIotAggregateKeep struct {
	InfluxIot
	interval time.Duration
}

func NewInfluxQLIotAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateKeep{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func NewFluxIotAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateKeep{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func (d *InfluxIotAggregateKeep) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.IotAggregateKeep(q, d.interval)
	return q
}
