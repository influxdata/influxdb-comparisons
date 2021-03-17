package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardSystemLoad produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardSystemLoad struct {
	InfluxDashboard
}

func NewInfluxQLDashboardSystemLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardSystemLoad{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardSystemLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardSystemLoad{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardSystemLoad) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT max("load5"), max("n_cpus") FROM "telegraf"."default"."system" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: GROUP BY time(1m), "host"
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT max(\"load5\"), max(\"n_cpus\") FROM system WHERE cluster_id = '%s' and %s group by time(1m), hostname", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`max_load5 = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "load5" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: max, createEmpty: false) `+
			`|> group(columns: ["hostname"])  `+
			`|> keep(columns:["_time", "_value", "hostname"])`+"\n"+
			`max_ncpus = from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "n_cpus" and r.cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> aggregateWindow(every: 1m, fn: max, createEmpty: false) `+
			`|> group(columns: ["hostname"])  `+
			`|> keep(columns:["_time", "_value", "hostname"])`+"\n"+
			`join(tables: {max_load_5:max_load5,max_ncpus:max_ncpus},on: ["_time", "hostname"]) `+
			`|> keep(columns: ["_time", "_value_max_load5", "_value_max_ncpus", "hostname"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId(),
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) System Load (Load5), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
