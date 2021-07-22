package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxMetaqueryFieldKeys produces metaqueries that will return a list of all
// field keys associated with a measurement.
type InfluxMetaqueryFieldKeys struct {
	InfluxMetaquery
}

func NewInfluxQLMetaqueryFieldKeys(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryFieldKeys{
		InfluxMetaquery: *underlying,
	}
}

func NewFluxMetaqueryFieldKeys(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryFieldKeys{
		InfluxMetaquery: *underlying,
	}
}

func (d *InfluxMetaqueryFieldKeys) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MetaqueryFieldKeys(q)
	return q
}
