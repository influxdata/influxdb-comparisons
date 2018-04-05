package main

import "time"

// TimescaleDevops8Hosts1Hr produces Timescale-specific queries for the devops single-host case.
type TimescaleDevops8Hosts1Hr struct {
	TimescaleDevops
}

func NewTimescaleDevops8Hosts1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevops8Hosts1Hr{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevops8Hosts1Hr) Dispatch(i, scaleVar int) Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
