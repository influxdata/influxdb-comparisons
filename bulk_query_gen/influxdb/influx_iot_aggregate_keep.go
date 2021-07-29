package influxdb

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// InfluxIotAggregateKeep produces metaqueries that will test performance
// on Flux statements aggregate and keep
type InfluxIotAggregateKeep struct {
	InfluxIot
}

func NewInfluxQLIotAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateKeep{
		InfluxIot: *underlying,
	}
}

func NewFluxIotAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateKeep{
		InfluxIot: *underlying,
	}
}

func (d *InfluxIotAggregateKeep) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.IotAggregateKeep(q)
	return q
}
