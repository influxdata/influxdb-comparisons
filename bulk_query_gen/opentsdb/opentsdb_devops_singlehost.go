package opentsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// OpenTSDBDevopsSingleHost produces OpenTSDB-specific queries for the devops single-host case.
type OpenTSDBDevopsSingleHost struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevopsSingleHost(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(dbConfig, start, end).(*OpenTSDBDevops)
	return &OpenTSDBDevopsSingleHost{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevopsSingleHost) Dispatch(i, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
