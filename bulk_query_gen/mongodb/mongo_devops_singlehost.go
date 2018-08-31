package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost struct {
	MongoDevops
}

func NewMongoDevopsSingleHost(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewMongoDevops(queriesFullRange, queryInterval, scaleVar).(*MongoDevops)
	return &MongoDevopsSingleHost{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
