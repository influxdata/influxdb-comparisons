package opentsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// OpenTSDBDevopsSingleHost12hr produces OpenTSDB-specific queries for the devops single-host case over a 12hr period.
type OpenTSDBDevopsSingleHost12hr struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevopsSingleHost12hr(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(queriesFullRange, queryInterval, scaleVar).(*OpenTSDBDevops)
	return &OpenTSDBDevopsSingleHost12hr{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
