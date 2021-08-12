package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxGroupWindowTransposeQuery struct {
	InfluxCommon
	aggregate   Aggregate
	interval    time.Duration
	cardinality Cardinality
}

func NewInfluxGroupWindowTransposeQuery(agg Aggregate, card Cardinality, lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxGroupWindowTransposeQuery{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], queriesFullRange, scaleVar),
		aggregate:    agg,
		interval:     queryInterval,
		cardinality:  card,
	}
}

func (d *InfluxGroupWindowTransposeQuery) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	if d.cardinality == LowCardinality {
		d.GroupWindowTransposeQuery(q)
	} else {
		d.GroupWindowTransposeCardinalityQuery(q)
	}
	return q
}

func (d *InfluxGroupWindowTransposeQuery) GroupWindowTransposeQuery(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(time.Hour * 6)

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT %s(temperature) FROM air_condition_room WHERE time > '%s' AND time < '%s' GROUP BY time(%s),home_id",
			d.aggregate, interval.StartString(), interval.EndString(), d.interval)
	} else {
		query = fmt.Sprintf(`from(bucket:"%s")
            |> range(start:%s, stop:%s)
            |> filter(fn:(r) => r._measurement == "air_condition_room" and r._field == "temperature")
						|> group(columns:["home_id"])
            |> aggregateWindow(every:%s, fn:%s)
            |> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.interval, d.aggregate)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) %s temperature, grouped on home_id", d.language.String(), d.aggregate)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}

func (d *InfluxGroupWindowTransposeQuery) GroupWindowTransposeCardinalityQuery(qi bulkQuerygen.Query) {
	interval := d.AllInterval.RandWindow(time.Hour * 12)

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf("SELECT %s(val) FROM example_measurement WHERE time > '%s' AND time < '%s' GROUP BY time(%s),X",
			d.aggregate, interval.StartString(), interval.EndString(), d.interval)
	} else {
		query = fmt.Sprintf(`from(bucket:"%s")
            |> range(start:%s, stop:%s)
            |> filter(fn:(r) => r._measurement == "example_measurement" and r._field == "val")
						|> group(columns:["X"])
            |> aggregateWindow(every:%s, fn:%s)
            |> yield()`,
			d.DatabaseName,
			interval.StartString(), interval.EndString(),
			d.interval, d.aggregate)
	}

	humanLabel := fmt.Sprintf("InfluxDB (%s) %s val, grouped on X, high cardinality", d.language.String(), d.aggregate)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, interval.StartString(), query, q)
}
