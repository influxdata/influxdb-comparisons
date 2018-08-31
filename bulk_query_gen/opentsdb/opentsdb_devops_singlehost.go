package opentsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// OpenTSDBDevopsSingleHost produces OpenTSDB-specific queries for the devops single-host case.
type OpenTSDBDevopsSingleHost struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevopsSingleHost(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*OpenTSDBDevops)
	return &OpenTSDBDevopsSingleHost{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
