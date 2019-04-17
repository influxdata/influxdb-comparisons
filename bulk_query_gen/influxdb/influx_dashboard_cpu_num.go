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
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"n_cpus\") FROM system WHERE cluster_id = '%s' and %s group by time(1m))", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "system" and r._field == "n_cpus" and r_cluster_id == "%s") `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> window(every:1m) `+ // TODO replace with aggregateWindow when it is fixed
			`|> max() `+
			`|> duplicate(column: "_stop", as: "_time") `+
			`|> window(every: inf) `+ //
			`|> last() `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) CPU (Number), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
