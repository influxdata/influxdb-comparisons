package mongodb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevops8Hosts1Hr struct {
	MongoDevops
}

func NewMongoDevops8Hosts1Hr(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewMongoDevops(queriesFullRange, queryInterval, scaleVar).(*MongoDevops)
	return &MongoDevops8Hosts1Hr{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevops8Hosts1Hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
