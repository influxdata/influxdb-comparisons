package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxMetaqueryCardinality struct {
	InfluxMetaquery
}

func NewInfluxQLMetaqueryCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryCardinality{
		InfluxMetaquery: *underlying,
	}
}

func NewFluxMetaqueryCardinality(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryCardinality{
		InfluxMetaquery: *underlying,
	}
}

func (d *InfluxMetaqueryCardinality) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MetaqueryCardinality(q)
	return q
}
