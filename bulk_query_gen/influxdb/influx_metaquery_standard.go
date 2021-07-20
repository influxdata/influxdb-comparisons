package influxdb

import (
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxMetaqueryStandard struct {
	InfluxMetaquery
}

func NewInfluxQLMetaqueryStandard(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryStandard{
		InfluxMetaquery: *underlying,
	}
}

func NewFluxMetaqueryStandard(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxMetaqueryCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMetaquery)
	return &InfluxMetaqueryStandard{
		InfluxMetaquery: *underlying,
	}
}
