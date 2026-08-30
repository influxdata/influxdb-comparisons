package influxdb

import (
	"fmt"
	"net/url"

	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
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

type Aggregate string

const (
	Count Aggregate = "count"
	Sum   Aggregate = "sum"
	Mean  Aggregate = "mean"
	Min   Aggregate = "min"
	Max   Aggregate = "max"
	First Aggregate = "first"
	Last  Aggregate = "last"
)

type Cardinality string

const (
	HighCardinality Cardinality = "high-card"
	LowCardinality  Cardinality = "low-card"
)

type InfluxCommon struct {
	bulkQuerygen.CommonParams
	language     Language
	DatabaseName string
	UserName     string
	Password     string
}

func newInfluxCommon(lang Language, dbName string, userName string, password string, interval bulkQuerygen.TimeInterval, scaleVar int) *InfluxCommon {
	return &InfluxCommon{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		language:     lang,
		DatabaseName: dbName,
		UserName: userName,
		Password: password}
}

// getHttpQuery gets the right kind of http request based on the language being used
func (d *InfluxCommon) getHttpQuery(humanLabel, intervalStart, query string, q *bulkQuerygen.HTTPQuery) {
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, intervalStart))
	q.Language = d.language.String()

	if d.language == InfluxQL {
		getValues := url.Values{}
		getValues.Set("db", d.DatabaseName)
		getValues.Set("q", query)
		if len(d.UserName) != 0 && len(d.Password) != 0 {
			getValues.Set("u", d.UserName)
			getValues.Set("p", d.Password)
		}
		q.Method = []byte("GET")
		q.Path = []byte(fmt.Sprintf("/query?%s", getValues.Encode()))
		q.Body = nil
	} else {
		q.Method = []byte("POST")
		//q.Path will be set in query_benchmarker_influxdb
		q.Body = []byte(query)
	}
}
