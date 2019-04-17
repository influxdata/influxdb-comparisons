package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardDiskUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardDiskUtilization struct {
	InfluxDashboard
}

func NewInfluxQLDashboardDiskUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUtilization{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardDiskUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUtilization{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardDiskUtilization) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT max("used_percent") FROM "telegraf"."default"."disk" WHERE "cluster_id" = :Cluster_Id: AND "path" = '/influxdb/conf' AND time > :dashboardTime: AND host =~ /.data./ GROUP BY time(1m), "host"
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT max(\"used_percent\") FROM disk WHERE cluster_id = '%s' and \"path\" = '/dev/sda1' and %s AND hostname =~ /data/ group by time(1m), \"hostname\"", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "disk" and r._field == "used_percent" and r._cluster_id == "%s" and r.path == "/dev/sda1" and hostname =~ /data/) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value", "hostname"]) `+
			`|> group(columns: ["hostname"]) `+
			`|> window(every: 1m) `+ // TODO replace window-max-window with aggregateWindow(every: 1m, fn: max) when it works
			`|> max() `+
			`|> window(every: inf) `+
			`|> keep(columns: ["_time","_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Utilization (Percent), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
