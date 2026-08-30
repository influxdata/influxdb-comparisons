package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxBareAggregateQuery struct {
	InfluxCommon
	aggregate Aggregate
}

func NewInfluxBareAggregateQuery(agg Aggregate, lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxBareAggregateQuery{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], dbConfig[bulkQuerygen.UserName], dbConfig[bulkQuerygen.Password], queriesFullRange, scaleVar),
		aggregate:    agg,
	}
}

func (d *InfluxBareAggregateQuery) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	d.BareAggregateQuery(q)
	return q
}

func (d *InfluxBareAggregateQuery) BareAggregateQuery(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(time.Hour * 6)

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT %s(temperature) FROM air_condition_room WHERE time >= '%s' AND time < '%s' GROUP BY home_id,room_id,sensor_id",
			d.aggregate, interval.StartString(), interval.EndString())
	} else {
		query = fmt.Sprintf(`from(bucket:"%s")
            |> range(start:%s, stop:%s)
            |> filter(fn:(r) => r._measurement == "air_condition_room" and r._field == "temperature")
            |> %s()
            |> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.aggregate)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) %s temperature, rand %s", d.language.String(), d.aggregate, interval.StartString())
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}
