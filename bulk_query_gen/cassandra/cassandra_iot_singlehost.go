package cassandra

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// CassandraIotSingleHost produces Cassandra-specific queries for the devops single-host case.
type CassandraIotSingleHost struct {
	CassandraIot
}

func NewCassandraIotSingleHost(dbConfig bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newCassandraIotCommon(dbConfig, queriesFullRange, queryInterval, scaleVar).(*CassandraIot)
	return &CassandraIotSingleHost{
		CassandraIot: *underlying,
	}
}

func (d *CassandraIotSingleHost) Dispatch(i int) bulkQuerygen.Query {
	q := NewCassandraQuery() // from pool
	d.AverageTemperatureDayByHourOneHome(q)
	return q
}
