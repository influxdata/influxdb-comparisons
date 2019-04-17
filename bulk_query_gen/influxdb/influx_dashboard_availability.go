package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardAvailability produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardAvailability struct {
	InfluxDashboard
}

func NewInfluxQLDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardAvailability) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT (sum("service_up") / count("service_up"))*100 AS "up_time" FROM "watcher"."autogen"."ping" WHERE cluster_id = :Cluster_Id: and time > :dashboardTime: FILL(linear)
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT (sum(\"service_up\") / count(\"service_up\"))*100 AS \"up_time\" FROM status WHERE cluster_id = '%s' and %s FILL(linear)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else { // TODO fill(linear) how??
		query = fmt.Sprintf(`data = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "status" and r._field == "service_up" and r._cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"])\n`+
			`sum = data |> sum()\n`+
			`count = data |> count()\n`+
			`join(tables:{sum:sum,count:count},on:["_time"]) `+
			`|> map(fn: (r) => ({_time:%s,_value:(float(v:r._value_sum) / float(v:r._value_count) * 100.0))) `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId(),
			interval.StartString())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Availability (Percent), rand cluster in %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
