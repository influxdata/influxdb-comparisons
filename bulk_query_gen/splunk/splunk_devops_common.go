package splunk

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

// SplunkDevops produces Influx-specific queries for all the devops query types.
type SplunkDevops struct {
	SplunkCommon
	DatabaseName string
}

// NewSplunkDevops makes an InfluxDevops object ready to generate Queries.
func newSplunkDevopsCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &SplunkDevops{
		SplunkCommon: *newSplunkCommon(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *SplunkDevops) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 2, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 4, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 8, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 16, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 32, time.Hour)
}

func (d *SplunkDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *SplunkDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	var hostnames []string
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname=%s", s))
	}
	combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

	query := fmt.Sprintf("| mstats max(_value) WHERE index=%s AND metric_name=usage_user AND (%s) earliest=%s latest=%s span=1m",
		d.DatabaseName, combinedHostnameClause, splunkTimestamp(interval.Start.UTC()), splunkTimestamp(interval.End.UTC()))
	humanLabel := fmt.Sprintf("Splunk max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)

	from := getTimestamp(interval.Start)
	until := getTimestamp(interval.End)

	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, from, until, query, q)
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *SplunkDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	query := fmt.Sprintf("| mstats avg(_value) WHERE index=%s AND metric_name=usage_user earliest=%s latest=%s span=1h BY hostname",
		d.DatabaseName, splunkTimestamp(interval.Start.UTC()), splunkTimestamp(interval.End.UTC()))
	humanLabel := fmt.Sprintf("Splunk mean cpu, all hosts, rand 1day by 1hour")

	from := getTimestamp(interval.Start)
	until := getTimestamp(interval.End)

	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, from, until, query, q)
}

func splunkTimestamp(t time.Time) string {
	return fmt.Sprintf("%02d/%02d/%04d:%02d:%02d:%02d", t.Month(), t.Day(), t.Year(), t.Hour(), t.Minute(), t.Second())
}