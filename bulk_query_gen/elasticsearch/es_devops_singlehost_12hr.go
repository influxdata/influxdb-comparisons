package elasticsearch

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// ElasticSearchDevopsSingleHost12hr produces ES-specific queries for the devops single-host case.
type ElasticSearchDevopsSingleHost12hr struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsSingleHost12hr(_ bulkQuerygen.DatabaseConfig, queriesFullRange bulkQuerygen.TimeInterval, queryInterval time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := NewElasticSearchDevops(queriesFullRange, scaleVar).(*ElasticSearchDevops)
	return &ElasticSearchDevopsSingleHost12hr{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsSingleHost12hr) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q)
	return q
}
