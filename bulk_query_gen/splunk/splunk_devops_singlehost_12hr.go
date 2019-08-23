package splunk

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// SplunkDevopsSingleHost12hr produces Influx-specific queries for the devops single-host case over a 12hr period.
type SplunkDevopsSingleHost12hr struct {
	SplunkDevops
}

func NewSplunkDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newSplunkDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*SplunkDevops)
	return &SplunkDevopsSingleHost12hr{
		SplunkDevops: *underlying,
	}
}

func (d *SplunkDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
