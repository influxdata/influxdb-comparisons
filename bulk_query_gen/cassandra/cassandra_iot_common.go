package cassandra

import (
	"fmt"
	bulkDataGenIot "github.com/influxdata/influxdb-comparisons/bulk_data_gen/iot"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"time"
)

// CassandraIot produces Cassandra-specific queries for all the devops query types.
type CassandraIot struct {
	KeyspaceName string
	AllInterval  bulkQuerygen.TimeInterval
}

// NewCassandraIot makes an CassandraIot object ready to generate Queries.
func newCassandraIotCommon(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &CassandraIot{
		KeyspaceName: dbConfig["database-name"],
		AllInterval:  bulkQuerygen.NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *CassandraIot) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *CassandraIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query, scaleVar int) {
	d.averageTemperatureDayByHourNHomes(q.(*CassandraQuery), scaleVar, 1, time.Hour)
}

// averageTemperatureHourByMinuteNHomes populates a Query with a query that looks like:
// SELECT avg(temperature) from air_condition_room where (home_id = '$HHOME_ID_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1h)
func (d *CassandraIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, scaleVar, nHomes int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nHomes]

	tagSets := [][]string{}
	tagSet := []string{}
	for _, n := range nn {
		home := fmt.Sprintf(bulkDataGenIot.SmartHomeIdFormat, n)
		tag := fmt.Sprintf("home_id = %s", home)
		tagSet = append(tagSet, tag)
	}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra average temperature, rand %4d homes, rand %s by 1h", nHomes, timeRange)
	q := qi.(*CassandraQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("avg")
	q.MeasurementName = []byte("air_condition_room")
	q.FieldName = []byte("temperature")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour

	q.TagSets = tagSets
}
