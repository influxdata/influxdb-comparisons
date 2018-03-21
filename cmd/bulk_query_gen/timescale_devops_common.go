package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// TimescaleDevops produces Timescale-specific queries for all the devops query types.
type TimescaleDevops struct {
	DatabaseName string
	AllInterval  TimeInterval
}

// newTimescaleDevopsCommon makes an TimescaleDevops object ready to generate Queries.
func newTimescaleDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}
	if _, ok := dbConfig["database-name"]; !ok {
		panic("need timescale database name")
	}

	return &TimescaleDevops{
		DatabaseName: dbConfig["database-name"],
		AllInterval:  NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TimescaleDevops) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 2, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 4, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 8, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 16, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 32, time.Hour)
}

func (d *TimescaleDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// select time_bucket(60000000000,time) as time1min,max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >=$HOUR_START and time < $HOUR_END group by time1min order by time1min;
func (d *TimescaleDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	humanLabel := fmt.Sprintf("Timescale max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)

	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.QuerySQL = []byte(fmt.Sprintf("select time_bucket(60000000000,time) as time1min,max(usage_user) from cpu where (%s) and time >=%d and time < %d group by time1min order by time1min", combinedHostnameClause, interval.StartUnixNano(), interval.EndUnixNano()))
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *TimescaleDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	humanLabel := "Timescale mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.QuerySQL = []byte(fmt.Sprintf("select time_bucket(3600000000000,time) as time1hour,avg(usage_user) from cpu where time >=%d and time < %d group by time1hour,hostname order by time1hour", interval.StartUnixNano(), interval.EndUnixNano()))
}
