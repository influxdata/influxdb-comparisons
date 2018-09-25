package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardThroughput produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardThroughput struct {
	InfluxDashboard
}

func NewInfluxQLDashboardThroughput(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardThroughput{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardThroughput(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardThroughput{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardThroughput) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT non_negative_derivative(max("pointReqLocal"), 10s) FROM "telegraf"."default"."influxdb_write" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host"
	query = fmt.Sprintf("SELECT non_negative_derivative(max(\"keyspace_hits\"), 10s) FROM redis WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by time(1m), \"hostname\"", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Per-Host Point Throughput (Number), %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
