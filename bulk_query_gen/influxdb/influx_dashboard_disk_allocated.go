package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardDiskAllocated produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardDiskAllocated struct {
	InfluxDashboard
}

func NewInfluxQLDashboardDiskAllocated(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskAllocated{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardDiskAllocated(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardDiskAllocated{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardDiskAllocated) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT last("max") from (SELECT max("total")/1073741824 FROM "telegraf"."default"."disk" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: and host =~ /.data./ GROUP BY time(120s))
	query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"total\")/1073741824 FROM disk WHERE cluster_id = '%s' and time >= '%s' and time < '%s' and hostname =~ /.data./ group by time(120s))", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Allocated (GB), rand cluster, %s by 120s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
