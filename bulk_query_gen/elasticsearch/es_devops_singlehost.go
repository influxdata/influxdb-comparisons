package elasticsearch

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// ElasticSearchDevopsSingleHost produces ES-specific queries for the devops single-host case.
type ElasticSearchDevopsSingleHost struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsSingleHost{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
