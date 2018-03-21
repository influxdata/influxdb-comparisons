package main

import "time"

// TimescaleDevopsSingleHost12hr produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost12hr struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost12hr{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
