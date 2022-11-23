package mongodb

import (
	"fmt"
	bulkQuerygen "github.com/influxdata/influxdb-comparisons/bulk_query_gen"
	"math/rand"
	"time"
)

// MongoDevops produces Mongo-specific queries for the devops use case.
type MongoDevops struct {
	bulkQuerygen.CommonParams
	DatabaseName string
}

// NewMongoDevops makes an MongoDevops object ready to generate Queries.
func NewMongoDevops(dbConfig bulkQuerygen.DatabaseConfig, interval bulkQuerygen.TimeInterval, duration time.Duration, scaleVar int) bulkQuerygen.QueryGenerator {
	return &MongoDevops{
		CommonParams: *bulkQuerygen.NewCommonParams(interval, scaleVar),
		DatabaseName: dbConfig[bulkQuerygen.DatabaseName],
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *MongoDevops) Dispatch(i int) bulkQuerygen.Query {
	q := NewMongoQuery() // from pool
	bulkQuerygen.DevopsDispatchAll(d, i, q, d.ScaleVar)
	return q
}

// MaxCPUUsageHourByMinuteOneHost populates a Query for getting the maximum CPU
// usage for one host over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 1, time.Hour)
}

// MaxCPUUsageHourByMinuteTwoHosts populates a Query for getting the maximum CPU
// usage for two hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 2, time.Hour)
}

// MaxCPUUsageHourByMinuteFourHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteFourHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 4, time.Hour)
}

// MaxCPUUsageHourByMinuteEightHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteEightHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 8, time.Hour)
}

// MaxCPUUsageHourByMinuteSixteenHosts populates a Query for getting the maximum CPU
// usage for four hosts over the course of an hour.
func (d *MongoDevops) MaxCPUUsageHourByMinuteSixteenHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 16, time.Hour)
}

func (d *MongoDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 32, time.Hour)
}

func (d *MongoDevops) MaxCPUUsage12HoursByMinuteOneHost(q bulkQuerygen.Query) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*MongoQuery), 1, 12*time.Hour)
}

func (d *MongoDevops) maxCPUUsageHourByMinuteNHosts(qi bulkQuerygen.Query, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(d.ScaleVar)[:nhosts]

	var hostnames []string
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	var hostnameMaps []M
	for _, h := range hostnames {
		if DocumentFormat == FlatFormat {
			// nothing to do
		} else {
			hostnameMaps = append(hostnameMaps, M{"key": "hostname", "val": h})
		}
	}

	var tagClause interface{}
	if DocumentFormat == FlatFormat {
		tagClause = hostnames
	} else {
		tagClause = hostnameMaps
	}

	var tagSpec string
	var fieldSpec, fieldPath string
	var fieldExpr interface{}
	if DocumentFormat == FlatFormat {
		tagSpec = "tags.hostname"
		fieldSpec = "fields.usage_user"
		fieldExpr = 1
		fieldPath = "fields.usage_user"
	} else {
		tagSpec = "tags"
		fieldSpec = "fields"
		fieldExpr = M{ "$filter": M{ "input": "$fields", "as": "field", "cond": M{ "$eq": []string{ "$$field.key", "usage_user" } } } }
		fieldPath = "fields.val"
	}

	var pipelineQuery []M
	if UseTimeseries {
		pipelineQuery = []M{
			{
				"$match": M{
					"tags.measurement": "cpu",
					"timestamp": M{
						"$gte": time.Unix(0, interval.StartUnixNano()),
						"$lt":  time.Unix(0, interval.EndUnixNano()),
					},
					tagSpec: M{
						"$in": tagClause,
					},
				},
			},
			{
				"$project": M{
					"_id": 0,
					"time_bucket": M{
						"$dateTrunc": M{
							"date": "$timestamp",
							"unit": "minute",
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
					"agg_value": M{"$max": "$" + fieldPath}, // was: $value
				},
			},
			{
				"$sort": M{"_id.time_bucket": 1},
			},
		}
	} else {
		bucketNano := time.Minute.Nanoseconds()
		pipelineQuery = []M{
			{
				"$match": M{
					"measurement": "cpu",
					"timestamp_ns": M{
						"$gte": interval.StartUnixNano(),
						"$lt":  interval.EndUnixNano(),
					},
					tagSpec: M{
						"$in": tagClause,
					},
				},
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
					"agg_value": M{"$max": "$" + fieldPath}, // was: $value
				},
			},
			{
				"$sort": M{"_id.time_bucket": 1},
			},
		}
	}

	humanLabel := []byte(fmt.Sprintf("Mongo max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange))
	q := qi.(*MongoQuery)
	q.HumanLabel = humanLabel
	q.BsonDoc = pipelineQuery
	q.DatabaseName = []byte(d.DatabaseName)
	q.CollectionName = []byte("point_data")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s, %s, %s, %s)", humanLabel, interval.StartString(), q.DatabaseName, q.CollectionName, q.MeasurementName, q.FieldName))
	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute
}

func (d *MongoDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi bulkQuerygen.Query) {
	//	if scaleVar > 10000 {
	//		// TODO: does this apply to mongo?
	//		panic("scaleVar > 10000 implies size > 10000, which is not supported on elasticsearch. see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html")
	//	}
	//
	//	interval := d.AllInterval.RandWindow(24 * time.Hour)
	//
	//	body := new(bytes.Buffer)
	//	mustExecuteTemplate(mongoFleetGroupByHostnameQuery, body, MongoFleetQueryParams{
	//		Start:         interval.StartString(),
	//		End:           interval.EndString(),
	//		Bucket:        "1h",
	//		Field:         "usage_user",
	//		HostnameCount: scaleVar,
	//	})
	//
	//	humanLabel := []byte("Mongo mean cpu, all hosts, rand 1day by 1hour")
	//	q := qi.(*HTTPQuery)
	//	q.HumanLabel = humanLabel
	//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	//	q.Method = []byte("POST")
	//
	//	q.Path = []byte("/cpu/_search")
	//	q.Body = body.Bytes()
}
