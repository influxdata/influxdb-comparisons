package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevopsGroupby produces Timescale-specific queries for the devops groupby case.
type TimescaleDevopsGroupby struct {
	TimescaleDevops
}

func NewTimescaleDevopsGroupby(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleDevops)
	return &TimescaleDevopsGroupby{
		TimescaleDevops: *underlying,
	}

}

func (d *TimescaleDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
