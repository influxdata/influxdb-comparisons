package cassandra

import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"time"
)

// CassandraDevops8Hosts produces Cassandra-specific queries for the devops groupby case.
type CassandraDevops8Hosts struct {
	CassandraDevops
}

func NewCassandraDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevops8Hosts{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevops8Hosts) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
