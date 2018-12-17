package cassandra

import (
	"fmt"
	"sync"
	"time"
)

// CassandraQuery encodes a Cassandra request. This will be serialized for use
// by the query_benchmarker program.
type CassandraQuery struct {
	HumanLabel       []byte
	HumanDescription []byte

	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	TagsCondition   []byte
}

var CassandraQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &CassandraQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			MeasurementName:  []byte{},
			FieldName:        []byte{},
			AggregationType:  []byte{},
			TagsCondition:    []byte{},
		}
	},
}

func NewCassandraQuery() *CassandraQuery {
	return CassandraQueryPool.Get().(*CassandraQuery)
}

// String produces a debug-ready description of a Query.
func (q *CassandraQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagSets: %s", q.HumanLabel, q.HumanDescription, q.MeasurementName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagsCondition)
}

func (q *CassandraQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *CassandraQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *CassandraQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.MeasurementName = q.MeasurementName[:0]
	q.FieldName = q.FieldName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.GroupByDuration = 0
	q.TimeStart = time.Time{}
	q.TimeEnd = time.Time{}
	q.TagsCondition = q.TagsCondition[:0]

	CassandraQueryPool.Put(q)
}
