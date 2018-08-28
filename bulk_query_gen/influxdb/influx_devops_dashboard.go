package influxdb

import "time"
import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
)

// InfluxDevopsDashboard produces Influx-specific queries for the devops single-host case.
type InfluxDevopsDashboard struct {
	InfluxDevops
}

var queries = []string{}

func NewInfluxQLDevopsDashboard(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(InfluxQL, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsDashboard{
		InfluxDevops: *underlying,
	}
}

func NewFluxDevopsDashboard(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDevopsCommon(Flux, dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsDashboard{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsDashboard) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool

	nhosts := 1
	timeRange := time.Hour * 24 * 365
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		if d.language == InfluxQL {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		} else {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf(`r.hostname == "%s"`, s))
		}
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	var query string
	query = fmt.Sprintf("SELECT max(usage_user) from cpu where (%s) and time >= '%s' and time < '%s' group by time(1m)", combinedHostnameClause, interval.StartString(), interval.EndString())

	humanLabel := fmt.Sprintf("InfluxDB (%s) max cpu, rand %4d hosts, rand %s by 1m", d.language.String(), nhosts, timeRange)

	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
	return q
}
