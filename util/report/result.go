package report

import (
	"time"
	"strconv"
	"fmt"
)

type LoadReportParams struct {
	DBType 	string
	DBVersion string
	ReportDatabaseName string
	ReportHost string
	ReportUser string
	ReportPassword string
	ReportTags [][2]string
	Hostname	string
	ParamIsGzip	bool
	ParamDestinationUrl string
	ParamReplicationFactor int
	ParamBatchSize int
	ParamWorkers	int
	ParamItemLimit	int
	ParamBackoff	time.Duration
	ParamConsistency string
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
	p.AddTag("server_url", params.ParamDestinationUrl)
	if len(params.DBType) > 0  {
		p.AddTag("database_type", params.DBType)
	}
	if len(params.DBVersion) > 0  {
		p.AddTag("database_version", params.DBVersion)
	}
	p.AddTag("item_limit", strconv.Itoa(params.ParamItemLimit))
	p.AddTag("gzip", strconv.FormatBool(params.ParamIsGzip))
	p.AddTag("batch_size", strconv.Itoa(params.ParamBatchSize))
	p.AddTag("workers", strconv.Itoa(params.ParamWorkers))

	p.AddTag("replication_factor", strconv.Itoa(params.ParamReplicationFactor))
	p.AddTag("back_off", strconv.Itoa(int(params.ParamBackoff.Seconds())))

	if len(params.ParamConsistency) > 0  {
		p.AddTag("consistency", params.ParamConsistency)
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