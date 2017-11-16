package report

import (
	"time"
	"strconv"
	"fmt"
)

type LoadReportParams struct {
	DBType             string
	DBVersion          string
	ReportDatabaseName string
	ReportHost         string
	ReportUser         string
	ReportPassword     string
	ReportTags         [][2]string
	Hostname           string
	DestinationUrl     string

	IsGzip            bool
	ReplicationFactor int
	BatchSize         int
	Workers           int
	ItemLimit         int
	Backoff           time.Duration
	Consistency       string
}

func ReportLoadResult(params *LoadReportParams, totalItems int64, valueRate float64, inputSpeed float64, loadDuration time.Duration) error {

	var authString string
	if len(params.ReportUser) > 0 {
		authString = fmt.Sprintf("%s:%s", params.ReportUser, params.ReportPassword)
	}
	c :=  NewCollector(params.ReportHost, params.ReportDatabaseName, authString)

	err := c.CreateDatabase()
	if err != nil {
		return err
	}

	p := GetPointFromGlobalPool()
	p.Init("load_benchmarks", time.Now().UnixNano())
	for _, tagpair := range params.ReportTags {
		p.AddTag(tagpair[0], tagpair[1])
	}
	p.AddTag("client_hostname", params.Hostname)
	p.AddTag("server_url", params.DestinationUrl)
	if len(params.DBType) > 0  {
		p.AddTag("database_type", params.DBType)
	}
	if len(params.DBVersion) > 0  {
		p.AddTag("database_version", params.DBVersion)
	}
	p.AddTag("item_limit", strconv.Itoa(params.ItemLimit))
	p.AddTag("gzip", strconv.FormatBool(params.IsGzip))
	p.AddTag("batch_size", strconv.Itoa(params.BatchSize))
	p.AddTag("workers", strconv.Itoa(params.Workers))

	p.AddTag("replication_factor", strconv.Itoa(params.ReplicationFactor))
	p.AddTag("back_off", strconv.Itoa(int(params.Backoff.Seconds())))

	if len(params.Consistency) > 0  {
		p.AddTag("consistency", params.Consistency)
	}


	p.AddInt64Field("total_items", totalItems)
	p.AddFloat64Field("values_rate", valueRate)
	p.AddFloat64Field("input_rate", inputSpeed)
	p.AddFloat64Field("duration", loadDuration.Seconds())

	c.Put(p)
	c.PrepBatch()

	err = c.SendBatch()

	PutPointIntoGlobalPool(p)

	return err

}