package report

import (
	"fmt"
	"strconv"
	"time"
)

// ReportParams is holder for common parameters across load and query reports
type ReportParams struct {
	DBType             string
	DBVersion          string
	ReportDatabaseName string
	ReportHost         string
	ReportUser         string
	ReportPassword     string
	ReportTags         [][2]string
	Hostname           string
	DestinationUrl     string
	Workers            int
	ItemLimit          int
}

// LoadReportParams is holder of bulk load specific parameters
type LoadReportParams struct {
	ReportParams

	IsGzip            bool
	ReplicationFactor int
	BatchSize         int
	Backoff           time.Duration
	Consistency       string
}

// type QueryReportParams is holder of bulk query specific parameters
type QueryReportParams struct {
	ReportParams

	BurnIn int64
}

// ReportLoadResult send results from bulk load to an influxdb according to the given parameters
func ReportLoadResult(params *LoadReportParams, totalItems int64, valueRate float64, inputSpeed float64, loadDuration time.Duration) error {

	c, p, err := initReport(&params.ReportParams, "load_benchmarks")

	p.AddTag("gzip", strconv.FormatBool(params.IsGzip))
	p.AddTag("batch_size", strconv.Itoa(params.BatchSize))

	p.AddTag("replication_factor", strconv.Itoa(params.ReplicationFactor))
	p.AddTag("back_off", strconv.Itoa(int(params.Backoff.Seconds())))

	if len(params.Consistency) > 0 {
		p.AddTag("consistency", params.Consistency)
	}

	p.AddInt64Field("total_items", totalItems)
	p.AddFloat64Field("values_rate", valueRate)
	p.AddFloat64Field("input_rate", inputSpeed)
	p.AddFloat64Field("duration", loadDuration.Seconds())

	err = finishReport(c, p)

	return err

}

// initReport prepares a Point and a Collector instance for sending a result report
func initReport(params *ReportParams, measurement string) (*Collector, *Point, error) {
	var authString string
	if len(params.ReportUser) > 0 {
		authString = fmt.Sprintf("%s:%s", params.ReportUser, params.ReportPassword)
	}
	c := NewCollector(params.ReportHost, params.ReportDatabaseName, authString)

	err := c.CreateDatabase()
	if err != nil {
		return nil, nil, err
	}

	p := GetPointFromGlobalPool()
	p.Init(measurement, time.Now().UnixNano())

	for _, tagpair := range params.ReportTags {
		p.AddTag(tagpair[0], tagpair[1])
	}

	p.AddTag("client_hostname", params.Hostname)
	p.AddTag("server_url", params.DestinationUrl)
	if len(params.DBType) > 0 {
		p.AddTag("database_type", params.DBType)
	}
	if len(params.DBVersion) > 0 {
		p.AddTag("database_version", params.DBVersion)
	}
	p.AddTag("item_limit", strconv.Itoa(params.ItemLimit))
	p.AddTag("workers", strconv.Itoa(params.Workers))

	return c, p, nil
}

//finishReport finalizes sending result report and cleaning data
func finishReport(c *Collector, p *Point) error {
	c.Put(p)
	c.PrepBatch()

	err := c.SendBatch()

	PutPointIntoGlobalPool(p)

	return err
}

//ReportQueryResult send result from bulk query benchmark to an influxdb according to the given parameters
func ReportQueryResult(params *QueryReportParams, minQueryTime float64, meanQueryTime float64, maxQueryTime float64, totalQueries int64, queryDuration time.Duration) error {

	c, p, err := initReport(&params.ReportParams, "query_benchmarks")

	p.AddTag("burn_in", strconv.Itoa(int(params.BurnIn)))

	p.AddFloat64Field("min_time", minQueryTime)
	p.AddFloat64Field("min_rate", 1000/minQueryTime)
	p.AddFloat64Field("mean_time", meanQueryTime)
	p.AddFloat64Field("mean_rate", 1000/meanQueryTime)
	p.AddFloat64Field("max_time", maxQueryTime)
	p.AddFloat64Field("max_rate", 1000/maxQueryTime)
	p.AddInt64Field("total_items", totalQueries)
	p.AddFloat64Field("duration", queryDuration.Seconds())

	err = finishReport(c, p)

	return err

}
