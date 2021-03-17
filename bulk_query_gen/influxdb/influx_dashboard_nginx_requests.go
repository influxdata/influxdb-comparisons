package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardNginxRequests produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardNginxRequests struct {
	InfluxDashboard
}

func NewInfluxQLDashboardNginxRequests(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardNginxRequests{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardNginxRequests(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardNginxRequests{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardNginxRequests) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT non_negative_derivative(mean("queriesExecuted"), 1s) FROM "telegraf"."default"."influxdb_queryExecutor" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host"
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT non_negative_derivative(mean(\"accepts\"), 1s) FROM nginx WHERE cluster_id = '%s' and %s group by time(1m), \"hostname\"", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "nginx" and r._field == "accepts" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns: ["hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: mean, createEmpty: false) `+
			`|> derivative(unit: 1s, nonNegative: true) `+
			`|> keep(columns: ["_time", "_value", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Queries Executed (Number)	, rand cluster, %s by 1m, host", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
