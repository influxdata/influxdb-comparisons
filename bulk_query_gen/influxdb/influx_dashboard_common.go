package influxdb

import (
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/dashboard"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboard produces Influx-specific queries for all the devops query types.
type InfluxDashboard struct {
	InfluxCommon
	ClustersCount int
}

// NewInfluxDashboard makes an InfluxDashboard object ready to generate Queries.
func newInfluxDashboard(lang Language, dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}
	clustersCount := dashboard.ClusterSizes[len(dashboard.ClusterSizes)/2]
	return &InfluxDashboard{
		InfluxCommon:  *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], interval, scaleVar),
		ClustersCount: clustersCount,
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxDashboard) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	//bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}
