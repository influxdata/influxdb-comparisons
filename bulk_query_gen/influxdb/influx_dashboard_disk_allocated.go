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
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT last(\"max\") from (SELECT max(\"total\")/1073741824 FROM disk WHERE cluster_id = '%s' and %s and hostname =~ /data/ group by time(120s))", d.GetRandomClusterId(), d.GetTimeConstraint(interval))
	} else {
		query = fmt.Sprintf(`from(bucket:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "disk" and r._field == "total" and r._cluster_id == "%s" and hostname =~ /data/) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> window(every: 120s) `+ // TODO replace with aggregateWindow when it is fixed
			`|> max() `+
			`|> window(every: inf) `+
			`|> map(fn: (r) => ({_time:r._time, _value:r._value / 1073741824})) `+
			`|> last() `+
			`|> keep(columns:["_time", "_value"]) `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.GetRandomClusterId())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) Disk Allocated (GB), rand cluster, %s by 120s", d.language.String(), interval.Duration())

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
