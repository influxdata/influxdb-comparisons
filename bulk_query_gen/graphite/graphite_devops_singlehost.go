package graphite

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// GraphiteDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type GraphiteDevopsSingleHost struct {
	GraphiteDevops
}

func NewGraphiteDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newGraphiteDevopsCommon(interval, duration, scaleVar).(*GraphiteDevops)
	return &GraphiteDevopsSingleHost{
		GraphiteDevops: *underlying,
	}
}

func (d *GraphiteDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
