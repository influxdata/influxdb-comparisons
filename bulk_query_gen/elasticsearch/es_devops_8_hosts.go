package elasticsearch

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// ElasticSearchDevops8Hosts produces ElasticSearch-specific queries for the devops groupby case.
type ElasticSearchDevops8Hosts struct {
	ElasticSearchDevops
}

func NewElasticSearchDevops8Hosts(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewElasticSearchDevops(queriesFullRange, scaleVar).(*ElasticSearchDevops)
	return &ElasticSearchDevops8Hosts{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevops8Hosts) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q)
	return q
}
