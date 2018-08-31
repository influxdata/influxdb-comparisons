package influxdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// InfluxDevopsGroupby produces Influx-specific queries for the devops groupby case.
type InfluxDevopsGroupby struct {
	InfluxDevops
}

func NewInfluxQLDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDevops)
	return &InfluxDevopsGroupby{
		InfluxDevops: *underlying,
	}

}

func NewFluxDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDevops)
	return &InfluxDevopsGroupby{
		InfluxDevops: *underlying,
	}

}

func (d *InfluxDevopsGroupby) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
