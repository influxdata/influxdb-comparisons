package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsSingleHost struct {
	InfluxDevops
}

func NewInfluxQLDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
