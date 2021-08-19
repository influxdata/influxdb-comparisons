package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxIotAggregateDrop produces queries that will test performance
// on Flux statements with drop() |> aggregateWindow()
type InfluxIotAggregateDrop struct {
	InfluxIot
	interval time.Duration
}

func NewInfluxQLIotAggregateDrop(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateDrop{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func NewFluxIotAggregateDrop(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotAggregateDrop{
		InfluxIot: *underlying,
		interval: queryInterval,
	}
}

func (d *InfluxIotAggregateDrop) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.IotAggregateDrop(q, d.interval)
	return q
}
