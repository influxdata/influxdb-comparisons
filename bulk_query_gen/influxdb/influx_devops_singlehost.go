package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsSingleHost struct {
	InfluxDevops
}

func NewInfluxQLDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
