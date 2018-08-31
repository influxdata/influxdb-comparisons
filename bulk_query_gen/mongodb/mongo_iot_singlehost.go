package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoIotSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoIotSingleHost struct {
	MongoIot
}

func NewMongoIotSingleHost(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewMongoIot(queriesFullRange, queryInterval, scaleVar).(*MongoIot)
	return &MongoIotSingleHost{
		MongoIot: *underlying,
	}
}

func (d *MongoIotSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q)
	return q
}
