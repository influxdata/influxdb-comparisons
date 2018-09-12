package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost12hr struct {
	MongoDevops
}

func NewMongoDevopsSingleHost12hr(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewMongoDevops(queriesFullRange, queryInterval, scaleVar).(*MongoDevops)
	return &MongoDevopsSingleHost12hr{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
