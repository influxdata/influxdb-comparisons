package splunk

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"net/url"
	"time"
)

type SplunkCommon struct {
	bulkQuerygen.CommonParams
}

func newSplunkCommon(interval bulkQuerygen.TimeInterval, scaleVar int) *SplunkCommon {
	return &SplunkCommon{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
	}
}

func (d *SplunkCommon) getHttpQuery(humanLabel, from, until, query string, q *bulkQuerygen.HTTPQuery) {
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s - %s", humanLabel, from, until))

	getValues := url.Values{}
	getValues.Set("search", query)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/services/search/jobs/export?%s&output_mode=json", getValues.Encode()))
	q.Body = nil
}

// TODO copy&pasted from Graphite - what good is this for???
func getTimestamp(t time.Time) string {
	return fmt.Sprintf("%02d:%02d_%04d%02d%02d", t.Hour(), t.Minute(), t.Year(), t.Month(), t.Day())
}
