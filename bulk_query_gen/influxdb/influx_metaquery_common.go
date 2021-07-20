package influxdb

import (
	"fmt"
	"time"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxMetaquery struct {
	InfluxCommon
}

func NewInfluxMetaqueryCommon(lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxMetaquery{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], queriesFullRange, scaleVar),
	}
}

func (d *InfluxMetaquery) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.MetaqueryDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *InfluxMetaquery) StandardMetaquery(qi bulkQuerygen.Query) {
	var query string
	if d.language == InfluxQL {
		query = `SHOW TAG VALUES FROM "example_measurement" WITH KEY = "X" LIMIT 200`
	} else {
		query = fmt.Sprintf(`from(bucket: "%s") `+
			`|> range(start: %s, stop: %s) `+
			`|> filter(fn: (r) => (r["_measurement"] == "example_measurement"))`+
			`|> keep(columns: ["X"])`+
			`|> group()`+
			`|> distinct(column: "X")`+
			`|> limit(n: 200)`+
			`|> sort()`,
			d.DatabaseName,
			d.AllInterval.StartString(),
			d.AllInterval.EndString(),
		)
	}

	humanLabel := fmt.Sprintf(`InfluxDB (%s) tag values for KEY = "X"`, d.language)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, "n/a", query, q)
}
