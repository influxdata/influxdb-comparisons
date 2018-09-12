package bulk_query_gen

import "time"

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch(int) Query
}

type QueryGeneratorMaker func(dbConfig DatabaseConfig, queriesFullRange TimeInterval, queryInterval time.Duration, scaleVar int) QueryGenerator
