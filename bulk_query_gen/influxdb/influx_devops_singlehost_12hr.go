package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevopsSingleHost12hr produces Influx-specific queries for the devops single-host case over a 12hr period.
type InfluxDevopsSingleHost12hr struct {
	InfluxDevops
}

func NewInfluxQLDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost12hr{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost12hr{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsSingleHost12hr) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
