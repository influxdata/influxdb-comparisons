package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardMemoryTotal produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardMemoryTotal struct {
	InfluxDashboard
}

func NewInfluxQLDashboardMemoryTotal(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardMemoryTotal{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardMemoryTotal(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardMemoryTotal{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardMemoryTotal) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT last("max") from (SELECT max("total")/1073741824 FROM "telegraf"."default"."mem" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: and host =~ /.data./ GROUP BY time(1m), host)
	query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"total\")/1073741824 FROM mem WHERE cluster_id = '%s' and time >= '%s' and time < '%s' and hostname =~ /.data./  group by time(1m), hostname)", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Memory (MB), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
