package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
)

// InfluxDashboardAvailability produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardAvailability struct {
	InfluxDashboard
	queryTimeRange time.Duration
}

func NewInfluxQLDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func NewFluxDashboardAvailability(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAvailability{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func (d *InfluxDashboardAvailability) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	interval := d.AllInterval.RandWindow(d.queryTimeRange)

	clusterId := fmt.Sprintf("%d", rand.Intn(15))
	var query string
	//SELECT (sum("service_up") / count("service_up"))*100 AS "up_time" FROM "watcher"."autogen"."ping" WHERE cluster_id = :Cluster_Id: and time > :dashboardTime: FILL(linear)
	query = fmt.Sprintf("SELECT (sum(\"service_up\") / count(\"service_up\"))*100 AS \"up_time\" FROM status WHERE cluster_id = '%s' and time >= '%s' and time < '%s' FILL(linear)", clusterId, interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Availability (Percent), rand cluster in %s", d.language.String(), d.queryTimeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
