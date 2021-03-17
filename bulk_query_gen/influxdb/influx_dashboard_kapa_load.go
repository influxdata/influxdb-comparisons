package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardKapaLoad produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardKapaLoad struct {
	InfluxDashboard
}

func NewInfluxQLDashboardKapaLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaLoad{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardKapaLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaLoad{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardKapaLoad) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT "load5", "load15", "load1" FROM "telegraf"."autogen"."system" WHERE time > :dashboardTime: AND "host"='kapacitor'
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT \"load5\", \"load15\", \"load1\" FROM system WHERE hostname='kapacitor_1' and %s", d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`load5 = from(bucket:"%s") `+ // TODO join 3 tables when it is supported?
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "load5" and r.hostname == "kapacitor_1") `+
			`|> keep(columns:["_time", "_value"])`+"\n"+
			`load15 = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "load15" and r.hostname == "kapacitor_1") `+
			`|> keep(columns:["_time", "_value"])`+"\n"+
			`load1 = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "load1" and r.hostname == "kapacitor_1") `+
			`|> keep(columns:["_time", "_value"])`+"\n"+
			`load5 |> yield(name: "load5")`+"\n"+
			`load15 |> yield(name: "load15")`+"\n"+
			`load1 |> yield(name: "load1")`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.DatabaseName,
			interval.StartString(), interval.EndString())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) kapa load 1,5,15 in %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
