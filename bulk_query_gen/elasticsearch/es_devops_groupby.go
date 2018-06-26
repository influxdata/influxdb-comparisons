package elasticsearch

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// ElasticSearchDevopsGroupBy produces ES-specific queries for the devops groupby case.
type ElasticSearchDevopsGroupBy struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsGroupBy(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsGroupBy{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsGroupBy) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
