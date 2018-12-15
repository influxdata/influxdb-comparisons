package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraDevopsSingleHost produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsSingleHost struct {
	CassandraDevops
}

func NewCassandraDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*CassandraDevops)
	return &CassandraDevopsSingleHost{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q)
	return q
}
