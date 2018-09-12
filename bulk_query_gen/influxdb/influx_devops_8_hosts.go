package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevops8Hosts produces Influx-specific queries for the devops groupby case.
type InfluxDevops8Hosts struct {
	InfluxDevops
}

func NewInfluxQLDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxDevops)
	return &InfluxDevops8Hosts{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxDevops)
	return &InfluxDevops8Hosts{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
