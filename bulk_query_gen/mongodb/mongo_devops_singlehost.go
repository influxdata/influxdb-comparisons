package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost struct {
	MongoDevops
}

func NewMongoDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewMongoDevops(dbConfig, start, end).(*MongoDevops)
	return &MongoDevopsSingleHost{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
