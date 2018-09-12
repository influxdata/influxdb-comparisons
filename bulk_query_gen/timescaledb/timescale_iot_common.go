package timescaledb

import (
	"fmt"
	bulkDataGenIot "github.com/influxdata/influxdb-comparisons/bulk_data_gen/iot"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strings"
	"time"
)

// TimescaleDevops produces Timescale-specific queries for all the devops query types.
type TimescaleIot struct {
	bulkQuerygen.CommonParams
	DatabaseName string
}

// NewTimescaleDevops makes an TimescaleDevops object ready to generate Queries.
func NewTimescaleIotCommon(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &TimescaleIot{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TimescaleIot) Dispatch(i int) bulkQuerygen.Query {
	q := NewSQLQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *TimescaleIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query) {
	d.averageTemperatureDayByHourNHomes(q.(*SQLQuery), 1, time.Hour*6)
}

// averageTemperatureHourByMinuteNHomes populates a Query with a query that looks like:
// SELECT avg(temperature) from air_condition_room where (home_id = '$HHOME_ID_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1h)
func (d *TimescaleIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, nHomes int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nHomes]

	homes := []string{}
	for _, n := range nn {
		homes = append(homes, fmt.Sprintf(bulkDataGenIot.SmartHomeIdFormat, n))
	}

	homeClauses := []string{}
	for _, s := range homes {
		homeClauses = append(homeClauses, fmt.Sprintf("home_id = '%s'", s))
	}

	combinedHomesClause := strings.Join(homeClauses, " or ")

	humanLabel := fmt.Sprintf("Timescale mean temperature, rand %4d homes, rand %s by 1h", nHomes, timeRange)
	q := qi.(*SQLQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.QuerySQL = []byte(fmt.Sprintf("select time_bucket(3600000000000,time) as time1hour,max(temperature) from air_condition_room where (%s) and time >=%d and time < %d group by time1hour order by time1hour ", combinedHomesClause, interval.StartUnixNano(), interval.EndUnixNano()))

}

//func (d *TimescaleDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.DatabaseName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Timescale mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*bulkQuerygen.HTTPQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}
