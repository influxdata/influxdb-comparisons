package timescaledb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// TimescaleIotSingleHost produces Timescale-specific queries for the devops single-host case.
type TimescaleIotSingleHost struct {
	TimescaleIot
}

func NewTimescaleIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewTimescaleIotCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*TimescaleIot)
	return &TimescaleIotSingleHost{
		TimescaleIot: *underlying,
	}
}

func (d *TimescaleIotSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q)
	return q
}
