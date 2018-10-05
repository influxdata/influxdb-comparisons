package influxdb

import "time"
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
	query = fmt.Sprintf("SELECT non_negative_derivative(percentile(\"uptime_in_seconds\", 99)) / non_negative_derivative(max(total_connections_received)) FROM redis WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by hostname, time(1m)", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) HTTP Request Duration (99th %%), rand cluster, %s by host, 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
