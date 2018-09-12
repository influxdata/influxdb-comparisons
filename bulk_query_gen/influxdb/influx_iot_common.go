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
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func NewInfluxIotCommon(lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxIot{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], queriesFullRange, scaleVar),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxIot) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *InfluxIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query) {
	d.averageTemperatureDayByHourNHomes(q.(*bulkQuerygen.HTTPQuery), 1, time.Hour*6)
}

// averageTemperatureHourByMinuteNHomes populates a Query with a query that looks like:
// SELECT avg(temperature) from air_condition_room where (home_id = '$HHOME_ID_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1h)
func (d *InfluxIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, nHomes int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nHomes]

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
		query = fmt.Sprintf(`from(db:"%s") `+
			`|> range(start:%s, stop:%s) `+
			`|> filter(fn:(r) => r._measurement == "air_condition_room" and r._field == "temperature" and (%s)) `+
			`|> keep(columns:["_start", "_stop", "_time", "_value"]) `+
			`|> window(every:1h) `+
			`|> mean() `+
			`|> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			combinedHomesClause)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) mean temperature, rand %4d homes, rand %s by 1h", d.language.String(), nHomes, timeRange)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}
