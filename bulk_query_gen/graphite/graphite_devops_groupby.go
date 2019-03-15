package graphite

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// GraphiteDevopsGroupby produces Influx-specific queries for the devops groupby case.
type GraphiteDevopsGroupby struct {
	GraphiteDevops
}

func NewGraphiteDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newGraphiteDevopsCommon(interval, duration, scaleVar).(*GraphiteDevops)
	return &GraphiteDevopsGroupby{
		GraphiteDevops: *underlying,
	}

}

func (d *GraphiteDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
