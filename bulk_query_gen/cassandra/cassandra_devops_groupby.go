package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsGroupby struct {
	CassandraDevops
}

func NewCassandraDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsGroupby{
		CassandraDevops: *underlying,
	}

}

func (d *CassandraDevopsGroupby) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
