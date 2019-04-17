package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardRedisMemoryUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardRedisMemoryUtilization struct {
	InfluxDashboard
}

func NewInfluxQLDashboardRedisMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardRedisMemoryUtilization{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardRedisMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardRedisMemoryUtilization{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardRedisMemoryUtilization) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT mean("usage_percent") FROM "telegraf"."default"."docker_container_mem" WHERE "cluster_id" = :Cluster_Id: AND ("container_name" =~ /influxd.*/ OR "container_name" =~ /kap.*/) AND time > :dashboardTime: GROUP BY time(1m), "host", "container_name" fill(previous)
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT mean(\"used_memory\") FROM redis WHERE cluster_id = '%s' and %s group by time(1m),hostname, server fill(previous)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else { // TODO fill(previous)???
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "redis" and r._field == "used_memory" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname", "server"]) `+
			`|> group(columns: ["hostname", "server"]) `+
			`|> aggregateWindow(every: 1m, fn: mean, createEmpty: false) `+
			`|> keep(columns: ["_time", "_value", "hostname", "server"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Memory Utilization, rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
