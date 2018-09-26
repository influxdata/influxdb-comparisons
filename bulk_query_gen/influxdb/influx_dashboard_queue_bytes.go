package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardQueueBytes produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardQueueBytes struct {
	InfluxDashboard
}

func NewInfluxQLDashboardQueueBytes(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardQueueBytes{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardQueueBytes(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardQueueBytes{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardQueueBytes) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT mean("queueBytes") FROM "telegraf"."default"."influxdb_hh_processor" WHERE "cluster_id" = :Cluster_Id: AND time > :dashboardTime: GROUP BY time(1m), "host" fill(0)
	query = fmt.Sprintf("SELECT mean(\"temp_files\") FROM system WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by time(1m), hostname, fill(0)", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Hinted HandOff Queue Size (MB), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
