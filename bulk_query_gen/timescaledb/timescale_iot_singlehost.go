package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleIotSingleHost produces Timescale-specific queries for the devops single-host case.
type TimescaleIotSingleHost struct {
	TimescaleIot
}

func NewTimescaleIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewTimescaleIotCommon(dbConfig, start, end).(*TimescaleIot)
	return &TimescaleIotSingleHost{
		TimescaleIot: *underlying,
	}
}

func (d *TimescaleIotSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q, scaleVar)
	return q
}
