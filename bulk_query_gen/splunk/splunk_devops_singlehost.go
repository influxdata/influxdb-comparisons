package splunk

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// SplunkDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type SplunkDevopsSingleHost struct {
	SplunkDevops
}

func NewSplunkDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newSplunkDevopsCommon(dbConfig, interval, duration, scaleVar).(*SplunkDevops)
	return &SplunkDevopsSingleHost{
		SplunkDevops: *underlying,
	}
}

func (d *SplunkDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
