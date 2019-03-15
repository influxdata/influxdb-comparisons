package graphite

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// GraphiteDevops8Hosts produces Influx-specific queries for the devops groupby case.
type GraphiteDevops8Hosts struct {
	GraphiteDevops
}

func NewGraphiteDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newGraphiteDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*GraphiteDevops)
	return &GraphiteDevops8Hosts{
		GraphiteDevops: *underlying,
	}
}

func (d *GraphiteDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
