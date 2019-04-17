package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardKapaCpu produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardKapaCpu struct {
	InfluxDashboard
}

func NewInfluxQLDashboardKapaCpu(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaCpu{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardKapaCpu(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaCpu{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardKapaCpu) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT 100 - "usage_idle" FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m AND "cpu"='cpu-total' AND "host"='kapacitor'
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT 100 - \"usage_idle\" FROM cpu WHERE hostname='kapacitor_1' and %s", d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "cpu" and r._field == "usage_idle" and r.hostname == "kapacitor_1") `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> map(fn: (r) => ({_time:r._time,_value:100.0 - r._value)) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) kapa cpu in %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
