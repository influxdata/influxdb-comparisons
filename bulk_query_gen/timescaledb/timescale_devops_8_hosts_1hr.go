package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevops8Hosts1Hr produces Timescale-specific queries for the devops single-host case.
type TimescaleDevops8Hosts1Hr struct {
	TimescaleDevops
}

func NewTimescaleDevops8Hosts1Hr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleDevops)
	return &TimescaleDevops8Hosts1Hr{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevops8Hosts1Hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
