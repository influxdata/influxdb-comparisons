package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
)

// InfluxDashboardDiskUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardDiskUtilization struct {
	InfluxDashboard
	queryTimeRange time.Duration
}

func NewInfluxQLDashboardDiskUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUtilization{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func NewFluxDashboardDiskUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskUtilization{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func (d *InfluxDashboardDiskUtilization) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	interval := d.AllInterval.RandWindow(d.queryTimeRange)

	clusterId := fmt.Sprintf("%d", rand.Intn(15))
	var query string
	//SELECT max("used_percent") FROM "telegraf"."default"."disk" WHERE "cluster_id" = :Cluster_Id: AND "path" = '/influxdb/conf' AND time > :dashboardTime: AND host =~ /.data./ GROUP BY time(1m), "host"
	query = fmt.Sprintf("SELECT max(\"used_percent\") FROM disk WHERE cluster_id = '%s' and \"path\" = '/dev/sda1' and time >= '%s' and time < '%s' AND hostname =~ /.data./ group by time(1m), \"hostname\"", clusterId, interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Utilization (Percent), rand cluster, %s by 1m", d.language.String(), d.queryTimeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
