package main

import "time"

// TimescaleDevopsGroupby produces Timescale-specific queries for the devops groupby case.
type TimescaleDevopsGroupby struct {
	TimescaleDevops
}

func NewTimescaleDevopsGroupby(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsGroupby{
		TimescaleDevops: *underlying,
	}

}

func (d *TimescaleDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewSQLQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
