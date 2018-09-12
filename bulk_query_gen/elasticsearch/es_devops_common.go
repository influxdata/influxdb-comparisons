package elasticsearch

import (
	"bytes"
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"io"
	"math/rand"
	"strings"
	"text/template"
	"time"
)

var (
	fleetQuery, fleetGroupByHostnameQuery, hostsQuery *template.Template
)

func init() {
	fleetQuery = template.Must(template.New("fleetQuery").Parse(rawFleetQuery))
	fleetGroupByHostnameQuery = template.Must(template.New("fleetGroupByHostnameQuery").Parse(rawFleetGroupByHostnameQuery))
	hostsQuery = template.Must(template.New("hostsQuery").Parse(rawHostsQuery))
}

// ElasticSearchDevops produces ES-specific queries for the devops use case.
type ElasticSearchDevops struct {
	bulkQuerygen.CommonParams
}

// NewElasticSearchDevops makes an ElasticSearchDevops object ready to generate Queries.
func NewElasticSearchDevops(interval bulkQuerygen.TimeInterval, scaleVar int) bulkQuerygen.QueryGenerator {
	return &ElasticSearchDevops{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *ElasticSearchDevops) Dispatch(i int) bulkQuerygen.Query {
	q := bulkQuerygen.NewHTTPQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

// MaxCPUUsageHourByMinuteOneHost populates a Query for getting the maximum CPU
// usage for one host over the course of an hour.
func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, time.Hour)
}

// MaxCPUUsageHourByMinuteTwoHosts populates a Query for getting the maximum CPU
// usage for two hosts over the course of an hour.
func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 2, time.Hour)
}

// MaxCPUUsageHourByMinuteFourHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 4, time.Hour)
}

// MaxCPUUsageHourByMinuteEightHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 8, time.Hour)
}

// MaxCPUUsageHourByMinuteSixteenHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 16, time.Hour)
}

func (d *ElasticSearchDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 32, time.Hour)
}
func (d *ElasticSearchDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*bulkQuerygen.HTTPQuery), 1, 12*time.Hour)
}

func (d *ElasticSearchDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("\"%s\"", s))
	}

	combinedHostnameClause := fmt.Sprintf("[ %s ]", strings.Join(hostnameClauses, ", "))

	body := new(bytes.Buffer)
	mustExecuteTemplate(hostsQuery, body, HostsQueryParams{
		JSONEncodedHostnames: combinedHostnameClause,
		Start:                interval.StartString(),
		End:                  interval.EndString(),
		Bucket:               "1m",
		Field:                "usage_user",
	})

	humanLabel := []byte(fmt.Sprintf("Elastic max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange))
	q := qi.(*bulkQuerygen.HTTPQuery)
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/cpu/_search")
	q.Body = body.Bytes()
}

func (d *ElasticSearchDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	if d.ScaleVar > 10000 {
		panic("scaleVar > 10000 implies size > 10000, which is not supported on elasticsearch. see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html")
	}

	interval := d.AllInterval.RandWindow(24 * time.Hour)

	body := new(bytes.Buffer)
	mustExecuteTemplate(fleetGroupByHostnameQuery, body, FleetQueryParams{
		Start:         interval.StartString(),
		End:           interval.EndString(),
		Bucket:        "1h",
		Field:         "usage_user",
		HostnameCount: d.ScaleVar,
	})

	humanLabel := []byte("Elastic mean cpu, all hosts, rand 1day by 1hour")
	q := qi.(*bulkQuerygen.HTTPQuery)
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("POST")

	q.Path = []byte("/cpu/_search")
	q.Body = body.Bytes()
}

func mustExecuteTemplate(t *template.Template, w io.Writer, params interface{}) {
	err := t.Execute(w, params)
	if err != nil {
		panic(fmt.Sprintf("logic error in executing template: %s", err))
	}
}

type FleetQueryParams struct {
	Bucket, Start, End, Field string
	HostnameCount             int
}

type HostsQueryParams struct {
	JSONEncodedHostnames      string
	Bucket, Start, End, Field string
}

const rawFleetQuery = `
{
  "size" : 0,
  "aggs": {
    "result": {
      "filter": {
        "range": {
          "timestamp": {
            "gte": "{{.Start}}",
            "lt": "{{.End}}"
          }
        }
      },
      "aggs": {
        "result2": {
          "date_histogram": {
            "field": "timestamp",
            "interval": "{{.Bucket}}",
            "format": "yyyy-MM-dd-HH"
          },
          "aggs": {
            "avg_of_field": {
              "avg": {
                 "field": "{{.Field}}"
              }
            }
          }
        }
      }
    }
  }
}
`

const rawFleetGroupByHostnameQuery = `
{
  "size" : 0,
  "aggs": {
    "result": {
      "filter": {
        "range": {
          "timestamp": {
            "gte": "{{.Start}}",
            "lt": "{{.End}}"
          }
        }
      },
      "aggs": {
        "by_hostname": {
          "terms": {
            "size": {{.HostnameCount}},
            "field": "hostname"
	  },
          "aggs": {
            "result2": {
              "date_histogram": {
                "field": "timestamp",
                "interval": "{{.Bucket}}",
                "format": "yyyy-MM-dd-HH"
              },
              "aggs": {
                "avg_of_field": {
                  "avg": {
                     "field": "{{.Field}}"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
`

const rawHostsQuery = `
{
  "size":0,
  "aggs":{
    "result":{
      "filter":{
        "bool":{
          "filter":{
            "range":{
              "timestamp":{
                "gte":"{{.Start}}",
                "lt":"{{.End}}"
              }
            }
          },
          "should":[
            {
              "terms":{
                "hostname": {{.JSONEncodedHostnames }}
              }
            }
          ],
	  "minimum_should_match" : 1
        }
      },
      "aggs":{
        "result2":{
          "date_histogram":{
            "field":"timestamp",
            "interval":"{{.Bucket}}",
            "format":"yyyy-MM-dd-HH"
          },
          "aggs":{
            "max_of_field":{
              "max":{
                "field":"{{.Field}}"
              }
            }
          }
        }
      }
    }
  }
}
`
