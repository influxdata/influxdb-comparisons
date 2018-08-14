package influxdb

import (
	"fmt"
	bulkDataGenIot "github.com/influxdata/influxdb-comparisons/bulk_data_gen/iot"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

// InfluxDevops produces Influx-specific queries for all the devops query types.
type InfluxIot struct {
	InfluxCommon
	AllInterval  bulkQuerygen.TimeInterval
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func NewInfluxIotCommon(lang Language, dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}
	if _, ok := dbConfig["database-name"]; !ok {
		panic("need influx database name")
	}

	return &InfluxIot{
		InfluxCommon: *newInfluxCommon(lang, dbConfig["database-name"]),
		AllInterval:  bulkQuerygen.NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxIot) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *InfluxIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query, scaleVar int) {
	d.averageTemperatureDayByHourNHomes(q.(*bulkQuerygen.HTTPQuery), scaleVar, 1, time.Hour*6)
}

// averageTemperatureHourByMinuteNHomes populates a Query with a query that looks like:
// SELECT avg(temperature) from air_condition_room where (home_id = '$HHOME_ID_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1h)
func (d *InfluxIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, scaleVar, nHomes int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nHomes]

	homes := []string{}
	for _, n := range nn {
		homes = append(homes, fmt.Sprintf(bulkDataGenIot.SmartHomeIdFormat, n))
	}

	homeClauses := []string{}
	for _, s := range homes {
		if d.language == InfluxQL {
			homeClauses = append(homeClauses, fmt.Sprintf("home_id = '%s'", s))
		} else {
			homeClauses = append(homeClauses, fmt.Sprintf(`r.home_id == "%s"`, s))
		}
	}

	combinedHomesClause := strings.Join(homeClauses, " or ")

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT mean(temperature) from air_condition_room where (%s) and time >= '%s' and time < '%s' group by time(1h)", combinedHomesClause, interval.StartString(), interval.EndString())
	} else {
		query = fmt.Sprintf(`from(db:"%s") ` +
			`|> range(start:%s, stop:%s) ` +
			`|> filter(fn:(r) => r._measurement == "air_condition_room" and r._field == "temperature") ` +
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) ` +
			`|> window(every:1h) ` +
			`|> mean() ` +
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString())
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) mean temperature, rand %4d homes, rand %s by 1h", d.language.String(), nHomes, timeRange)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}

//func (d *InfluxDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.DatabaseName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Influx mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*bulkQuerygen.HTTPQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}
