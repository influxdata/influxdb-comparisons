package mongodb

import (
	"fmt"
	bulkDataGenIot "github.com/influxdata/influxdb-comparisons/bulk_data_gen/iot"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"time"
)

// MongoIot produces Mongo-specific queries for the devops use case.
type MongoIot struct {
	bulkQuerygen.CommonParams
	DatabaseName string
}

// NewMongoIot makes an MongoIot object ready to generate Queries.
func NewMongoIot(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &MongoIot{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *MongoIot) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	bulkQuerygen.IotDispatchAll(d, i, q, d.ScaleVar)
	return q
}

func (d *MongoIot) AverageTemperatureDayByHourOneHome(q bulkQuerygen.Query) {
	d.averageTemperatureDayByHourNHomes(q.(*MongoQuery), 1, time.Hour*12)
}

func (d *MongoIot) averageTemperatureDayByHourNHomes(qi bulkQuerygen.Query, nHomes int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nHomes]

	var homes []string
	for _, n := range nn {
		homes = append(homes, fmt.Sprintf(bulkDataGenIot.SmartHomeIdFormat, n))
	}

	var homeMaps []M
	for _, h := range homes {
		if DocumentFormat == FlatFormat {
			// nothing to do
		} else {
			homeMaps = append(homeMaps, M{"key": "home_id", "val": h})
		}
	}

	var tagClause interface{}
	if DocumentFormat == FlatFormat {
		tagClause = homes
	} else {
		tagClause = homeMaps
	}

	var tagSpec string
	var fieldSpec, fieldPath string
	var fieldExpr interface{}
	if DocumentFormat == FlatFormat {
		tagSpec = "tags.home_id"
		fieldSpec = "fields.temperature"
		fieldExpr = 1
		fieldPath = "fields.temperature"
	} else {
		tagSpec = "tags"
		fieldSpec = "fields"
		fieldExpr = M{ "$filter": M{ "input": "$fields", "as": "field", "cond": M{ "$eq": []string{ "$$field.key", "temperature" } } } }
		fieldPath = "fields.val"
	}

	var pipelineQuery []M
	if UseTimeseries {
		var match M
		if UseSingleCollection {
			match = M{
				"tags.measurement": "air_condition_room",
				"timestamp": M{
					"$gte": time.Unix(0, interval.StartUnixNano()),
					"$lt":  time.Unix(0, interval.EndUnixNano()),
				},
				tagSpec: M{
					"$in": tagClause,
				},
			}
		} else {
			match = M{
				"timestamp": M{
					"$gte": time.Unix(0, interval.StartUnixNano()),
					"$lt":  time.Unix(0, interval.EndUnixNano()),
				},
				tagSpec: M{
					"$in": tagClause,
				},
			}
		}
		pipelineQuery = []M{
			{
				"$match": match,
			},
			{
				"$project": M{
					"_id": 0,
					"time_bucket": M{
						"$dateTrunc": M{
							"date": "$timestamp",
							"unit": "hour",
						},
					},
					fieldSpec:     fieldExpr, // was value: 1
					//"measurement": 1, // why was this set?
				},
			},
			{
				"$unwind": "$fields",
			},
			{
				"$group": M{
					"_id":       M{"time_bucket": "$time_bucket", "tags": "$" + tagSpec}, // was: "$tags"
					"agg_value": M{"$avg": "$" + fieldPath}, // was: $value
				},
			},
			{
				"$sort": M{"_id.time_bucket": 1},
			},
		}
	} else {
		bucketNano := time.Hour.Nanoseconds()
		var match M
		if UseSingleCollection {
			match = M{
				"measurement": "air_condition_room",
				"timestamp_ns": M{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
				},
				tagSpec: M{
					"$in": tagClause,
				},
			}
		} else {
			match = M{
				"timestamp_ns": M{
					"$gte": interval.StartUnixNano(),
					"$lt":  interval.EndUnixNano(),
				},
				tagSpec: M{
					"$in": tagClause,
				},
			}
		}
		pipelineQuery = []M{
			{
				"$match": match,
			},
			{
				"$project": M{
					"_id": 0,
					"time_bucket": M{
						"$subtract": S{
							"$timestamp_ns",
							M{"$mod": S{"$timestamp_ns", bucketNano}},
						},
					},
					fieldSpec:     fieldExpr, // was value: 1
					//"measurement": 1, // why was this set?
				},
			},
			{
				"$unwind": "$fields",
			},
			{
				"$group": M{
					"_id":       M{"time_bucket": "$time_bucket", "tags": "$" + tagSpec}, // was: "$tags"
					"agg_value": M{"$avg": "$" + fieldPath}, // was: $value
				},
			},
			{
				"$sort": M{"_id.time_bucket": 1},
			},
		}
	}

	humanLabel := []byte(fmt.Sprintf("Mongo avg temperature, rand %4d homes, rand %s by 1h", nHomes, timeRange))
	q := qi.(*MongoQuery)
	q.HumanLabel = humanLabel
	q.BsonDoc = pipelineQuery
	q.DatabaseName = []byte(d.DatabaseName)
	if UseSingleCollection {
		q.CollectionName = []byte("point_data")
	} else {
		q.CollectionName = []byte("air_condition_room")
	}
	q.MeasurementName = []byte("air_condition_room")
	q.FieldName = []byte("temperature")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s, %s, %s, %s)", humanLabel, interval.StartString(), q.DatabaseName, q.CollectionName, q.MeasurementName, q.FieldName))
	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
}
