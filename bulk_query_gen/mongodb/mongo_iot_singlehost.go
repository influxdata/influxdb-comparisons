package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoIotSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoIotSingleHost struct {
	MongoIot
}

func NewMongoIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewMongoIot(dbConfig, start, end).(*MongoIot)
	return &MongoIotSingleHost{
		MongoIot: *underlying,
	}
}

func (d *MongoIotSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q, scaleVar)
	return q
}
