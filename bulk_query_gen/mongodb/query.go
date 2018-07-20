package mongodb

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"
)

type S []interface{}
type M map[string]interface{}

func init() {
	// needed for serializing the mongo query to gob
	gob.Register(S{})
	gob.Register(M{})
	gob.Register([]M{})
}

// MongoQuery encodes a Mongo request. This will be serialized for use
// by the query_benchmarker program.
type MongoQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	DatabaseName     []byte
	CollectionName   []byte
	BsonDoc          []M

	// these are only for debugging. the data is encoded in BsonDoc.
	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum"
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	TagSets         [][]string // semantically, each subgroup is OR'ed and they are all AND'ed together
}

var MongoQueryPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &MongoQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			DatabaseName:     []byte{},
			CollectionName:   []byte{},
			BsonDoc:          []M{},

			MeasurementName: []byte{},
			FieldName:       []byte{},
			AggregationType: []byte{},
			TagSets:         [][]string{},
		}
	},
}

func NewMongoQuery() *MongoQuery {
	return MongoQueryPool.Get().(*MongoQuery)
}

// String produces a debug-ready description of a Query.
func (q *MongoQuery) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagSets: %s", q.HumanLabel, q.HumanDescription, q.MeasurementName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagSets)
}

func (q *MongoQuery) HumanLabelName() []byte {
	return q.HumanLabel
}
func (q *MongoQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *MongoQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.DatabaseName = q.DatabaseName[:0]
	q.CollectionName = q.CollectionName[:0]
	q.BsonDoc = nil

	q.MeasurementName = q.MeasurementName[:0]
	q.FieldName = q.FieldName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.GroupByDuration = 0
	q.TimeStart = time.Time{}
	q.TimeEnd = time.Time{}
	q.TagSets = q.TagSets[:0]

	MongoQueryPool.Put(q)
}
