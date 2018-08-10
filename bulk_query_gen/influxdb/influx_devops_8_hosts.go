package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevops8Hosts produces Influx-specific queries for the devops groupby case.
type InfluxDevops8Hosts struct {
	InfluxDevops
}

func NewInfluxQLDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops8Hosts{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops8Hosts{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops8Hosts) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
