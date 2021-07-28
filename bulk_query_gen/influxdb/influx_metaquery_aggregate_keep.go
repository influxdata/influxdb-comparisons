package influxdb

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// InfluxMetaqueryAggregateKeep produces metaqueries that will test performance
// on Flux statements aggregate and keep
type InfluxMetaqueryAggregateKeep struct {
	InfluxMetaquery
}

func NewInfluxQLMetaqueryAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryAggregateKeep{
		InfluxMetaquery: *underlying,
	}
}

func NewFluxMetaqueryAggregateKeep(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryAggregateKeep{
		InfluxMetaquery: *underlying,
	}
}

func (d *InfluxMetaqueryAggregateKeep) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MetaqueryAggregateKeep(q)
	return q
}
