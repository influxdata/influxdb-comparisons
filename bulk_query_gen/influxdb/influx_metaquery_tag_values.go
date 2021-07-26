package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxMetaqueryTagValues produces metaqueries that will return a list of all
// tag values for a specific tag key name.
type InfluxMetaqueryTagValues struct {
	InfluxMetaquery
}

func NewInfluxQLMetaqueryTagValues(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryTagValues{
		InfluxMetaquery: *underlying,
	}
}

func NewFluxMetaqueryTagValues(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryTagValues{
		InfluxMetaquery: *underlying,
	}
}

func (d *InfluxMetaqueryTagValues) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MetaqueryTagValues(q)
	return q
}
