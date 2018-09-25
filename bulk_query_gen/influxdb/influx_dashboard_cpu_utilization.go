package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardCpuUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardCpuUtilization struct {
	InfluxDashboard
}

func NewInfluxQLDashboardCpuUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuUtilization{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardCpuUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuUtilization{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardCpuUtilization) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//c "telegraf"."default"."cpu" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: GROUP BY host, time(1m)
	query = fmt.Sprintf("SELECT mean(\"usage_user\") FROM cpu WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by hostname,time(1m)", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) CPU Utilization (Percent), rand cluster, %s by host, 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
