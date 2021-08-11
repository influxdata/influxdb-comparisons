package influxdb

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// InfluxIotSortedPivot produces queries that will test performance
// on Flux pivot function
type InfluxIotSortedPivot struct {
	InfluxIot
	interval time.Duration
}

func NewInfluxQLIotSortedPivot(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotSortedPivot{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func NewFluxIotSortedPivot(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotSortedPivot{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func (d *InfluxIotSortedPivot) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	d.IotSortedPivot(q, d.interval)
	return q
}
