package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxIotBatteryLevels struct {
	InfluxIot
	interval time.Duration
}

func NewInfluxQLIotBatteryLevels(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotBatteryLevels{
		InfluxIot: *underlying,
		interval:  queryInterval,
	}
}

func NewFluxIotBatteryLevels(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxIot)
	return &InfluxIotBatteryLevels{
		InfluxIot: *underlying,
		interval:  queryInterval,
	}
}

func (d *InfluxIotBatteryLevels) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.BatteryLevels(q, d.interval)
	return q
}
