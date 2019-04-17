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
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT non_negative_derivative(max(\"keyspace_hits\"), 10s) FROM redis WHERE cluster_id = '%s' and %s group by time(1m), \"hostname\"", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "redis" and r._field == "keyspace_hits" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns: ["hostname"]) `+
			`|> window(every: 1m) `+ // TODO replace with aggregateWindow when it is fixed
			`|> max() `+
			`|> window(every: inf) `+
			`|> derivative(unit: 10s, nonNegative: true) `+
			`|> keep(columns: ["_time", "_value", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Per-Host Point Throughput (Number), %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
