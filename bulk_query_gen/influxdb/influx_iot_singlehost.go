package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxIotSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxIotSingleHost struct {
	InfluxIot
}

func NewInfluxQLIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotSingleHost{
		InfluxIot: *underlying,
	}
}

func NewFluxIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotSingleHost{
		InfluxIot: *underlying,
	}
}

func (d *InfluxIotSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q)
	return q
}
