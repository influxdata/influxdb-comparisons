package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleDevopsSingleHost12hr produces Timescale-specific queries for the devops single-host case.
type TimescaleDevopsSingleHost12hr struct {
	TimescaleDevops
}

func NewTimescaleDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newTimescaleDevopsCommon(dbConfig, start, end).(*TimescaleDevops)
	return &TimescaleDevopsSingleHost12hr{
		TimescaleDevops: *underlying,
	}
}

func (d *TimescaleDevopsSingleHost12hr) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
