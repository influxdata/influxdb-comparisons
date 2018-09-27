package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardCpuNum produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardCpuNum struct {
	InfluxDashboard
}

func NewInfluxQLDashboardCpuNum(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuNum{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardCpuNum(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardCpuNum{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardCpuNum) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT last("max") from (SELECT max("n_cpus") FROM "telegraf"."default"."system" WHERE time > :dashboardTime: and cluster_id = :Cluster_Id: GROUP BY time(1m))
	query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"n_cpus\") FROM system WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by time(1m))", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) CPU (Number), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
