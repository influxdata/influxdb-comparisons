package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type Aggregate string

const (
	Count Aggregate = "count"
	Sum   Aggregate = "sum"
	Mean  Aggregate = "mean"
	Min   Aggregate = "min"
	Max   Aggregate = "max"
	First Aggregate = "first"
	Last  Aggregate = "last"
)

type InfluxWindowAggregateQuery struct {
	InfluxCommon
	aggregate Aggregate
	interval  time.Duration
}

func NewInfluxWindowAggregateQuery(agg Aggregate, lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxWindowAggregateQuery{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], queriesFullRange, scaleVar),
		aggregate:    agg,
		interval:     queryInterval,
	}
}

func (d *InfluxWindowAggregateQuery) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	d.WindowAggregateQuery(q)
	return q
}

func (d *InfluxWindowAggregateQuery) WindowAggregateQuery(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(time.Hour * 6)

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT %s(temperature) FROM air_condition_room WHERE time > '%s' AND time < '%s' GROUP BY time(%s)",
			d.aggregate, interval.StartString(), interval.EndString(), d.interval)
	} else {
		query = fmt.Sprintf(`from(bucket:"%s")
            |> range(start:%s, stop:%s)
            |> filter(fn:(r) => r._measurement == "air_condition_room" and r._field == "temperature")
            |> aggregateWindow(every:%s, fn:%s)
            |> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.interval, d.aggregate)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) %s temperature, rand %s by %s", d.language.String(), d.aggregate, interval.StartString(), d.interval)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}
