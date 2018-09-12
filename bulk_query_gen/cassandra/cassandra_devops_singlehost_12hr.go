package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraDevopsSingleHost12hr produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsSingleHost12hr struct {
	CassandraDevops
}

func NewCassandraDevopsSingleHost12hr(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*CassandraDevops)
	return &CassandraDevopsSingleHost12hr{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
