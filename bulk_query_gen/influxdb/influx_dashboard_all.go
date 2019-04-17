package influxdb

import "time"
import (
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
)

// InfluxDashboardAll produces Influx-specific queries for the dashboard single-host case.
type InfluxDashboardAll struct {
	InfluxDashboard
	Gens []bulkQuerygen.QueryGenerator
}

func NewInfluxQLDashboardAll(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(InfluxQL, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAll{
		InfluxDashboard: *underlying,
		Gens: []bulkQuerygen.QueryGenerator{
			NewInfluxQLDashboardAvailability(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardCpuNum(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardCpuUtilization(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardDiskAllocated(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardDiskUsage(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardDiskUtilization(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardHttpRequestDuration(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardHttpRequests(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardKapaCpu(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardKapaLoad(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardKapaRam(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardMemoryTotal(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardMemoryUtilization(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardNginxRequests(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardQueueBytes(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardRedisMemoryUtilization(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardSystemLoad(dbConfig, interval, duration, scaleVar),
			NewInfluxQLDashboardThroughput(dbConfig, interval, duration, scaleVar),
		},
	}
}

func NewFluxDashboardAll(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	underlying := newInfluxDashboard(Flux, dbConfig, interval, duration, scaleVar).(*InfluxDashboard)
	return &InfluxDashboardAll{
		InfluxDashboard: *underlying,
		Gens: []bulkQuerygen.QueryGenerator{
			NewFluxDashboardAvailability(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardCpuNum(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardCpuUtilization(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardDiskAllocated(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardDiskUsage(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardDiskUtilization(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardHttpRequestDuration(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardHttpRequests(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardKapaCpu(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardKapaLoad(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardKapaRam(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardMemoryTotal(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardMemoryUtilization(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardNginxRequests(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardQueueBytes(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardRedisMemoryUtilization(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardSystemLoad(dbConfig, interval, duration, scaleVar),
			NewFluxDashboardThroughput(dbConfig, interval, duration, scaleVar),
		},
	}
}

func (d *InfluxDashboardAll) Dispatch(i int) bulkQuerygen.Query {
	return d.Gens[i%len(d.Gens)].Dispatch(i)
}
