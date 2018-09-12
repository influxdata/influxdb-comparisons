package influxdb

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"net/url"
)

type Language bool

const (
	InfluxQL Language = false
	Flux     Language = true
)

func (lang Language) String() string {
	if lang == InfluxQL {
		return "InfluxQL"
	} else {
		return "Flux"
	}
}

type InfluxCommon struct {
	bulkQuerygen.CommonParams
	language     Language
	DatabaseName string
}

func newInfluxCommon(lang Language, dbName string, interval bulkQuerygen.TimeInterval, scaleVar int) *InfluxCommon {
	return &InfluxCommon{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		language:     lang,
		DatabaseName: dbName}
}

// getHttpQuery gets the right kind of http request based on the language being used
func (d *InfluxCommon) getHttpQuery(humanLabel, intervalStart, query string, q *bulkQuerygen.HTTPQuery) {
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, intervalStart))

	getValues := url.Values{}
	if d.language == InfluxQL {
		getValues.Set("db", d.DatabaseName)
		getValues.Set("q", query)
		q.Method = []byte("GET")
		q.Path = []byte(fmt.Sprintf("/query?%s", getValues.Encode()))
		q.Body = nil
	} else {
		postValues := url.Values{}
		postValues.Set("query", query)
		getValues.Set("organization", "my-org")
		q.Method = []byte("POST")
		q.Path = []byte(fmt.Sprintf("/query?%s", getValues.Encode()))
		q.Body = []byte(postValues.Encode())
	}
}
