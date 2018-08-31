package elasticsearch

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// ElasticSearchDevopsGroupBy produces ES-specific queries for the devops groupby case.
type ElasticSearchDevopsGroupBy struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsGroupBy(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewElasticSearchDevops(queriesFullRange, scaleVar).(*ElasticSearchDevops)
	return &ElasticSearchDevopsGroupBy{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsGroupBy) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q)
	return q
}
