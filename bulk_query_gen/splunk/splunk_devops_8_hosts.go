package splunk

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// SplunkDevops8Hosts produces Influx-specific queries for the devops groupby case.
type SplunkDevops8Hosts struct {
	SplunkDevops
}

func NewSplunkDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newSplunkDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*SplunkDevops)
	return &SplunkDevops8Hosts{
		SplunkDevops: *underlying,
	}
}

func (d *SplunkDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
