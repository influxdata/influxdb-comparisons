package splunk

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// SplunkDevopsGroupby produces Influx-specific queries for the devops groupby case.
type SplunkDevopsGroupby struct {
	SplunkDevops
}

func NewSplunkDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newSplunkDevopsCommon(dbConfig, interval, duration, scaleVar).(*SplunkDevops)
	return &SplunkDevopsGroupby{
		SplunkDevops: *underlying,
	}

}

func (d *SplunkDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
