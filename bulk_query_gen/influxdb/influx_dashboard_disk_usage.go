package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardDiskUsage produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardDiskUsage struct {
	InfluxDashboard
}

func NewInfluxQLDashboardDiskUsage(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUsage{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardDiskUsage(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUsage{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardDiskUsage) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT last("used_percent") AS "mean_used_percent" FROM "telegraf"."default"."disk" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: and host =~ /.data./
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT last(\"used_percent\") AS \"mean_used_percent\" FROM disk WHERE cluster_id = '%s' and %s and hostname =~ /data/", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "disk" and r._field == "used_percent" and r_cluster_id == "%s" and hostname =~ /data/) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> last() `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Usage (GB), rand cluster, %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
