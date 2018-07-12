package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraIotSingleHost produces Cassandra-specific queries for the devops single-host case.
type CassandraIotSingleHost struct {
	CassandraIot
}

func NewCassandraIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newCassandraIotCommon(dbConfig, start, end).(*CassandraIot)
	return &CassandraIotSingleHost{
		CassandraIot: *underlying,
	}
}

func (d *CassandraIotSingleHost) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q, scaleVar)
	return q
}
