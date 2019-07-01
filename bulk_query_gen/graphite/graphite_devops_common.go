package graphite

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

// GraphiteDevops produces Influx-specific queries for all the devops query types.
type GraphiteDevops struct {
	GraphiteCommon
}

// NewGraphiteDevops makes an InfluxDevops object ready to generate Queries.
func newGraphiteDevopsCommon(interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &GraphiteDevops{
		GraphiteCommon: *newGraphiteCommon(interval, scaleVar),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *GraphiteDevops) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 2, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 4, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 8, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 16, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 32, time.Hour)
}

func (d *GraphiteDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *GraphiteDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	hostnamesNumbers := []string{}
	for _, n := range nn {
		hostnamesNumbers = append(hostnamesNumbers, fmt.Sprintf("%d", n))
	}
	var hostnamesRegexp string
	if len(hostnamesNumbers) == 1 {
		hostnamesRegexp = fmt.Sprintf("%s", hostnamesNumbers[0])
	} else {
		hostnamesRegexp = fmt.Sprintf("(%s)", strings.Join(hostnamesNumbers, "|"))
	}
	hostnameClause := fmt.Sprintf("'hostname=host_%s'", hostnamesRegexp)

	query := fmt.Sprintf("summarize(seriesByTag('name=cpu.usage_user',%s),'1min',func='max',alignToFrom=True)", hostnameClause)
	humanLabel := fmt.Sprintf("Graphite max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)

	from := getTimestamp(interval.Start)
	until := getTimestamp(interval.End)

	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, from, until, query, q)
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *GraphiteDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	query := fmt.Sprintf("summarize(seriesByTag('name=cpu.usage_user'),'1min',func='mean',alignToFrom=True) | groupByTags('hostname')")
	humanLabel := fmt.Sprintf("Graphite mean cpu, all hosts, rand 1day by 1hour")

	from := getTimestamp(interval.Start)
	until := getTimestamp(interval.End)

	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, from, until, query, q)
}
