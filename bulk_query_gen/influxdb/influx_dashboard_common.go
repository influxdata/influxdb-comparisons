package influxdb

import (
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/dashboard"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"strconv"
	"time"
)

// InfluxDashboard produces Influx-specific queries for all the devops query types.
type InfluxDashboard struct {
	InfluxCommon
	ClustersCount int
	bulkQuerygen.TimeWindow
}

// NewInfluxDashboard makes an InfluxDashboard object ready to generate Queries.
func newInfluxDashboard(lang Language, dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	if _, ok := dbConfig[bulkQuerygen.DatabaseName]; !ok {
		panic("need influx database name")
	}
	clustersCount := scaleVar / dashboard.ClusterSize //ClusterSizes[len(dashboard.ClusterSizes)/2]
	if clustersCount == 0 {
		clustersCount = 1
	}
	version, err := strconv.Atoi(dbConfig["influxVersion"])
	if err != nil {
		panic("invalid influx version")
	}
	return &InfluxDashboard{
		InfluxCommon:  *newInfluxCommon(lang, dbConfig[bulkQuerygen.DatabaseName], interval, scaleVar, version),
		ClustersCount: clustersCount,
		TimeWindow:    bulkQuerygen.TimeWindow{interval.Start, duration},
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxDashboard) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	//bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *InfluxDashboard) DispatchCommon(i int) (*bulkQuerygen.HTTPQuery, *bulkQuerygen.TimeInterval) {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	var interval bulkQuerygen.TimeInterval
	if bulkQuerygen.TimeWindowShift > 0 {
		interval = d.TimeWindow.SlidingWindow(&d.AllInterval)
	} else {
		interval = d.AllInterval.RandWindow(d.Duration)
	}
	return q, &interval
}

func (d *InfluxDashboard) GetTimeConstraint(interval *bulkQuerygen.TimeInterval) string {
	var s string
	switch bulkQuerygen.QueryIntervalType {
	case "window":
		s = fmt.Sprintf("time >= '%s' and time < '%s'", interval.StartString(), interval.EndString())
	case "last":
		s = fmt.Sprintf("time >= now() - %dh and time < now() - %dh", int64(2*interval.Duration().Hours()), int64(interval.Duration().Hours()))
	case "recent":
		s = fmt.Sprintf("time >= now() - %dh and time < now() - %dh", int64(interval.Duration().Hours()+24), int64(24))
	}
	return s
}

func (d *InfluxDashboard) GetRandomClusterId() string {
	return fmt.Sprintf("%d", rand.Intn(d.ClustersCount-1)+1)
}
