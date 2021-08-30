package influxdb

import (
	"fmt"
	"math/rand"
	"time"

	multiMeasurement "github.com/influxdata/influxdb-comparisons/bulk_data_gen/multi_measurement"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

type InfluxMultiMeasurement struct {
	InfluxCommon
}

func InfluxMultiMeasurementCommon(lang Language, dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}

	return &InfluxMultiMeasurement{
		InfluxCommon: *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], queriesFullRange, scaleVar),
	}
}

func (d *InfluxMultiMeasurement) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	return q
}

type InfluxMultiMeasurementOr struct {
	InfluxMultiMeasurement
	interval time.Duration
}

func NewInfluxQLMultiMeasurementOr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := InfluxMultiMeasurementCommon(InfluxQL, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMultiMeasurement)
	return &InfluxMultiMeasurementOr{
		InfluxMultiMeasurement: *underlying,
		interval:               queryInterval,
	}
}

func NewFluxMultiMeasurementOr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := InfluxMultiMeasurementCommon(Flux, dbConfig, queriesFullRange, queryInterval, scaleVar).(*InfluxMultiMeasurement)
	return &InfluxMultiMeasurementOr{
		InfluxMultiMeasurement: *underlying,
		interval:               queryInterval,
	}
}

func (d *InfluxMultiMeasurementOr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery()
	d.MultiMeasurementOr(q, d.interval)
	return q
}

func (d *InfluxMultiMeasurementOr) MultiMeasurementOr(qi bulkQuerygen.Query, queryInterval time.Duration) {
	randField := fmt.Sprintf(multiMeasurement.FieldSig, rand.Intn(multiMeasurement.NumFields))
	meas1 := fmt.Sprintf(multiMeasurement.MeasSig, rand.Intn(d.ScaleVar*multiMeasurement.MeasMultiplier))
	meas2 := fmt.Sprintf(multiMeasurement.MeasSig, rand.Intn(d.ScaleVar*multiMeasurement.MeasMultiplier))

	var query string
	if d.language == InfluxQL {
		query = fmt.Sprintf(`SELECT "%s" FROM "%s", "%s" WHERE time > '%s' AND time < '%s'`,
			randField,
			meas1, meas2,
			d.AllInterval.StartString(), d.AllInterval.EndString(),
		)
	} else {
		query = fmt.Sprintf(`from(bucket: "%s") 
			|> range(start: %s, stop: %s) 
			|> filter(fn: (r) => r["_measurement"] == "%s" or r["_measurement"] == "%s") 
			|> filter(fn: (r) => r["_field"] == "%s")`,
			d.DatabaseName,
			d.AllInterval.StartString(), d.AllInterval.EndString(),
			meas1, meas2,
			randField,
		)
	}

	humanLabel := fmt.Sprintf(`InfluxDB (%s) Multi-Measure Query`, d.language)
	q := qi.(*bulkQuerygen.HTTPQuery)
	d.getHttpQuery(humanLabel, "n/a", query, q)
}
