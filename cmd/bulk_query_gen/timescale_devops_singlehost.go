package main

import "time"

// TimescaleDevopsSingleHost produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost) Dispatch(i, scaleVar int) Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
