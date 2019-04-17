package influxdb

import (
	"time"
)
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardHttpRequestDuration produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardHttpRequestDuration struct {
	InfluxDashboard
}

func NewInfluxQLDashboardHttpRequestDuration(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardHttpRequestDuration{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardHttpRequestDuration(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardHttpRequestDuration{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardHttpRequestDuration) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT non_negative_derivative(percentile("writeReqDurationNs", 99)) /  non_negative_derivative(max(writeReq)) FROM "telegraf"."default"."influxdb_httpd" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY host, time(1m)
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT non_negative_derivative(percentile(\"uptime_in_seconds\", 99)) / non_negative_derivative(max(total_connections_received)) FROM redis WHERE cluster_id = '%s' and %s group by hostname, time(1m)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`ndp_uptime = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "redis" and r._field == "uptime_in_seconds" and r._cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> map(fn: (r) => ({_time:r._time,_start:r._start,_stop:r._stop,_time:r._time,_value:float(v:r._value),hostname:r.hostname})) `+
			`|> group(columns: ["hostname"]) `+
			`|> window(every: 1m) `+
			`|> quantile(q: 0.99, method: "estimate_tdigest") `+
			`|> duplicate(column: "_stop", as: "_time") `+
			`|> window(every: inf) `+
			`|> derivative(nonNegative: true)\n`+
			`ndp_uptime = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "redis" and r._field == "uptime_in_seconds" and r._cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> map(fn: (r) => ({_time:r._time,_start:r._start,_stop:r._stop,_time:r._time,_value:float(v:r._value),hostname:r.hostname})) `+
			`|> group(columns: ["hostname"]) `+
			`|> window(every: 1m) `+
			`|> max() `+
			`|> drop(columns: ["_time"]) `+
			`|> duplicate(column: "_stop", as: "_time") `+
			`|> window(every: inf) `+
			`|> derivative(nonNegative: true)\n`+
			`join(tables:{ndp_uptime:ndp_uptime,ndm_conn:ndm_conn},on: ["_time","hostname"]) `+
			`|> map(fn: (r) => ({_time:r._time,_value:(r._value_ndp_uptime / r._value_ndm_conn)})) `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId(),
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) HTTP Request Duration (99th %%), rand cluster, %s by host, 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
