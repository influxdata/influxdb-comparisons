package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraDevopsSingleHost produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsSingleHost struct {
	CassandraDevops
}

func NewCassandraDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsSingleHost{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsSingleHost) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
