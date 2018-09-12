package opentsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// OpenTSDBDevops8Hosts produces OpenTSDB-specific queries for the devops groupby case.
type OpenTSDBDevops8Hosts struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevops8Hosts(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*OpenTSDBDevops)
	return &OpenTSDBDevops8Hosts{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
