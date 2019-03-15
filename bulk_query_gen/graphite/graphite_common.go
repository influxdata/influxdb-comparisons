package graphite

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"net/url"
	"time"
)

type GraphiteCommon struct {
	bulkQuerygen.CommonParams
}

func newGraphiteCommon(interval bulkQuerygen.TimeInterval, scaleVar int) *GraphiteCommon {
	return &GraphiteCommon{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
	}
}

func (d *GraphiteCommon) getHttpQuery(humanLabel, from, until, query string, q *bulkQuerygen.HTTPQuery) {
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s - %s", humanLabel, from, until))

	getValues := url.Values{}
	getValues.Set("target", query)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/render?%s&format=json&noNullPoints=true&from=%s&until=%s", getValues.Encode(), from, until))
	q.Body = nil
}

func getTimestamp(t time.Time) string {
	return fmt.Sprintf("%02d:%02d_%04d%02d%02d", t.Hour(), t.Minute(), t.Year(), t.Month(), t.Day())
}
