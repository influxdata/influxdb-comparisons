package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost12hr struct {
	MongoDevops
}

func NewMongoDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewMongoDevops(dbConfig, start, end).(*MongoDevops)
	return &MongoDevopsSingleHost12hr{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost12hr) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
