package report

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ReportParams is holder for common parameters across load and query reports
type ReportParams struct {
	DBType             string
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

	IsGzip    bool
	BatchSize int
}

// type QueryReportParams is holder of bulk query specific parameters
type QueryReportParams struct {
	ReportParams

	BurnIn int64
}

// ReportLoadResult send results from bulk load to an influxdb according to the given parameters
func ReportLoadResult(params *LoadReportParams, totalItems int64, valueRate float64, inputSpeed float64, loadDuration time.Duration) error {

	c, p, err := initReport(&params.ReportParams, "load_benchmarks")
	if err != nil {
		return err
	}

	p.AddTag("gzip", strconv.FormatBool(params.IsGzip))
	p.AddTag("batch_size", strconv.Itoa(params.BatchSize))

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
	p.AddTag("server_url", strings.Replace(params.DestinationUrl, ",", "\\,", -1))
	if len(params.DBType) > 0 {
		p.AddTag("database_type", params.DBType)
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

const escapes = "\t\n\f\r ,="

var escaper = strings.NewReplacer(
	"\t", `\t`,
	"\n", `\n`,
	"\f", `\f`,
	"\r", `\r`,
	`,`, `\,`,
	` `, `\ `,
	`=`, `\=`,
)

func Escape(s string) string {
	if strings.ContainsAny(s, escapes) {
		return escaper.Replace(s)
	} else {
		return s
	}
}

//ReportQueryResult send result from bulk query benchmark to an influxdb according to the given parameters
func ReportQueryResult(params *QueryReportParams, queryName string, minQueryTime float64, meanQueryTime float64, maxQueryTime float64, totalQueries int64, movingMean float64, queryDuration time.Duration) error {

	c, p, err := initReport(&params.ReportParams, "query_benchmarks")
	if err != nil {
		return err
	}

	p.AddTag("burn_in", strconv.Itoa(int(params.BurnIn)))
	p.AddTag("query_name", Escape(queryName))

	p.AddFloat64Field("min_time", minQueryTime)
	if minQueryTime > 0 {
		p.AddFloat64Field("min_rate", 1000/minQueryTime)
	} else {
		p.AddFloat64Field("min_rate", -1)
	}
	p.AddFloat64Field("mean_time", meanQueryTime)
	if meanQueryTime > 0 {
		p.AddFloat64Field("mean_rate", 1000/meanQueryTime)
	} else {
		p.AddFloat64Field("mean_rate", -1)
	}
	p.AddFloat64Field("max_time", maxQueryTime)
	if maxQueryTime > 0 {
		p.AddFloat64Field("max_rate", 1000/maxQueryTime)
	} else {
		p.AddFloat64Field("max_rate", -1)
	}
	p.AddFloat64Field("moving_mean_time", movingMean)
	p.AddInt64Field("total_items", totalQueries)
	p.AddFloat64Field("duration", queryDuration.Seconds())

	err = finishReport(c, p)

	return err

}
