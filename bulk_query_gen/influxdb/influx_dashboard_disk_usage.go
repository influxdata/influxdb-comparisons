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
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration,scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUsage{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardDiskUsage) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT last("used_percent") AS "mean_used_percent" FROM "telegraf"."default"."disk" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: and host =~ /.data./
	query = fmt.Sprintf("SELECT last(\"used_percent\") AS \"mean_used_percent\" FROM disk WHERE cluster_id = '%s' and time >= '%s' and time < '%s' and hostname =~ /.data./", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Usage (GB), rand cluster, %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
