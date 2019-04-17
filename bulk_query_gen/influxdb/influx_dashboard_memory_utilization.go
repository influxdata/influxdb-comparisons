package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardMemoryUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardMemoryUtilization struct {
	InfluxDashboard
}

func NewInfluxQLDashboardMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardMemoryUtilization{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardMemoryUtilization{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardMemoryUtilization) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT mean("used_percent") FROM "telegraf"."default"."mem" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host"
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT mean(\"used_percent\") FROM mem WHERE cluster_id = '%s' and %s group by time(1m), hostname", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "mem" and r._field == "used_percent" and r_cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns:["hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: mean, createEmpty: false) ` +
			`|> keep(columns: ["_time", "_value", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Memory Utilization (Percent), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
