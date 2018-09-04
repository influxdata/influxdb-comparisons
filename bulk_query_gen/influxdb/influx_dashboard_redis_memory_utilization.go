package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
)

// InfluxDashboardRedisMemoryUtilization produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardRedisMemoryUtilization struct {
	InfluxDashboard
	queryTimeRange time.Duration
}

func NewInfluxQLDashboardRedisMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardRedisMemoryUtilization{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func NewFluxDashboardRedisMemoryUtilization(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardRedisMemoryUtilization{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func (d *InfluxDashboardRedisMemoryUtilization) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	interval := d.AllInterval.RandWindow(d.queryTimeRange)

	clusterId := fmt.Sprintf("%d", rand.Intn(15))
	var query string
	//SELECT mean("usage_percent") FROM "telegraf"."default"."docker_container_mem" WHERE "cluster_id" = :Cluster_Id: AND ("container_name" =~ /influxd.*/ OR "container_name" =~ /kap.*/) AND time > :dashboardTime: GROUP BY time(1m), "host", "container_name" fill(previous)
	query = fmt.Sprintf("SELECT mean(\"used_memory\") FROM redis WHERE cluster_id = '%s' and time >= '%s' and time < '%s' group by time(1m),hostname, server fill(previous)", clusterId, interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) Memory Utilization, rand cluster, %s by 1m", d.language.String(), d.queryTimeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
