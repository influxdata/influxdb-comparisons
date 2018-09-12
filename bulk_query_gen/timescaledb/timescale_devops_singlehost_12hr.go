package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevopsSingleHost12hr produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost12hr struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost12hr{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
