package opentsdb

import "time"
import bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"

// OpenTSDBDevops8Hosts produces OpenTSDB-specific queries for the devops groupby case.
type OpenTSDBDevops8Hosts struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevops8Hosts(dbConfig bulkQuerygen.DatabaseConfig, start, end time.Time) bulkQuerygen.QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(dbConfig, start, end).(*OpenTSDBDevops)
	return &OpenTSDBDevops8Hosts{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevops8Hosts) Dispatch(_, scaleVar int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
