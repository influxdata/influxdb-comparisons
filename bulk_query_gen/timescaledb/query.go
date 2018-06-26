package timescaledb

import (
	"fmt"
	"sync"
)

var SQLQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &SQLQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			QuerySQL:         []byte{},
		}
	},
}

// SQLQuery encodes an full constructed SQL query. This will typically by serialized for use
// by the query_benchmarker program.
type SQLQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	QuerySQL         []byte
}

func NewSQLQuery() *SQLQuery {
	return SQLQueryPool.Get().(*SQLQuery)
}

// String produces a debug-ready description of a Query.
func (q *SQLQuery) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Query: \"%s\"", q.HumanLabel, q.HumanDescription, q.QuerySQL)
}

func (q *SQLQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *SQLQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *SQLQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.QuerySQL = q.QuerySQL[:0]

	SQLQueryPool.Put(q)
}
