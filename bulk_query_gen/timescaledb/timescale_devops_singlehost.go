package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevopsSingleHost produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
