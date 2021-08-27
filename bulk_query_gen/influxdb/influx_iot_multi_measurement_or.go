package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxIotMultiMeasurementOr struct {
	InfluxIot
	interval time.Duration
}

func NewInfluxQLIotMultiMeasurementOr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotMultiMeasurementOr{
		InfluxIot: *underlying,
		interval:  queryInterval,
	}
}

func NewFluxIotMultiMeasurementOr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotMultiMeasurementOr{
		InfluxIot: *underlying,
		interval:  queryInterval,
	}
}

func (d *InfluxIotMultiMeasurementOr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MultiMeasurementOr(q, d.interval)
	return q
}
