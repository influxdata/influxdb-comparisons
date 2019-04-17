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
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"total\")/1073741824 FROM mem WHERE cluster_id = '%s' and %s and hostname =~ /data/  group by time(1m), hostname)", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "mem" and r._field == "total" and r._cluster_id == "%s" and hostname =~ /data/) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> group(columns: ["hostname"]) `+
			`|> window(every: 1m) `+ // TODO replace window-max-window with aggregateWindow(every: 1m, fn: max) when it works
			`|> max() `+
			`|> window(every: inf) `+
			`|> map(fn: (r) => ({_time:r._time, _value:r._value / 1073741824})) `+
			`|> group() `+ // or add 'group by hostname' to InfluxQL to return last max for each host
			`|> last() `+
			`|> keep(columns: ["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Memory (MB), rand cluster, %s by 1m", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
