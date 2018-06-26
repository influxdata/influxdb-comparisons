package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevopsGroupby produces Timescale-specific queries for the devops groupby case.
type TimescaleDevopsGroupby struct {
	TimescaleDevops
}

func NewTimescaleDevopsGroupby(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsGroupby{
		TimescaleDevops: *underlying,
	}

}

func (d *TimescaleDevopsGroupby) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
