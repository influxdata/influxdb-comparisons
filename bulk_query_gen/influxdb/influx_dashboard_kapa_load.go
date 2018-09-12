package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardKapaLoad produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardKapaLoad struct {
	InfluxDashboard
	queryTimeRange time.Duration
}

func NewInfluxQLDashboardKapaLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaLoad{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func NewFluxDashboardKapaLoad(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardKapaLoad{
		InfluxDashboard: *underlying,
		queryTimeRange:  duration,
	}
}

func (d *InfluxDashboardKapaLoad) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	interval := d.AllInterval.RandWindow(d.queryTimeRange)

	var query string
	//SELECT "load5", "load15", "load1" FROM "telegraf"."autogen"."system" WHERE time > :dashboardTime: AND "host"='kapacitor'
	query = fmt.Sprintf("SELECT \"load5\", \"load15\", \"load1\" FROM system WHERE hostname='kapacitor' and time >= '%s' and time < '%s'", interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) kapa load 1,5,15 in %s", d.language.String(), d.queryTimeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
