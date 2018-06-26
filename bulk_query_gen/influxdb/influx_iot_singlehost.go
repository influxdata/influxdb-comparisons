package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxIotSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxIotSingleHost struct {
	InfluxIot
}

func NewInfluxIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewInfluxIotCommon(dbConfig, start, end).(*InfluxIot)
	return &InfluxIotSingleHost{
		InfluxIot: *underlying,
	}
}

func (d *InfluxIotSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q, scaleVar)
	return q
}
