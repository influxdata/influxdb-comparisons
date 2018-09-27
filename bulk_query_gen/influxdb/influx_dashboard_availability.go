package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardAvailability produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardAvailability struct {
	InfluxDashboard
}

func NewInfluxQLDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
	}
}

func NewFluxDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
	}
}

func (d *InfluxDashboardAvailability) Dispatch(i int) bulkQuerygen.Query {
	q, interval := d.InfluxDashboard.DispatchCommon(i)

	var query string
	//SELECT (sum("service_up") / count("service_up"))*100 AS "up_time" FROM "watcher"."autogen"."ping" WHERE cluster_id = :Cluster_Id: and time > :dashboardTime: FILL(linear)
	query = fmt.Sprintf("SELECT (sum(\"service_up\") / count(\"service_up\"))*100 AS \"up_time\" FROM status WHERE cluster_id = '%s' and time >= '%s' and time < '%s' FILL(linear)", d.GetRandomClusterId(), interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Availability (Percent), rand cluster in %s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
