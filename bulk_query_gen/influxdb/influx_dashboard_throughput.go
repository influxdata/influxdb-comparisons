package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardThroughput produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardThroughput struct {
	InfluxDashboard
	queryTimeRange time.Duration
}

func NewInfluxQLDashboardThroughput(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardThroughput{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func NewFluxDashboardThroughput(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardThroughput{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func (d *InfluxDashboardThroughput) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	interval := d.AllInterval.RandWindow(d.queryTimeRange)

	var query string
	//SELECT non_negative_derivative(max("pointReqLocal"), 10s) FROM "telegraf"."default"."influxdb_write" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host"
	query = fmt.Sprintf("SELECT non_negative_derivative(max(\"keyspace_hits\"), 10s) FROM redis WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by time(1m), \"hostname\"", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Per-Host Point Throughput (Number), %s by 1m", d.language.String(), d.queryTimeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
