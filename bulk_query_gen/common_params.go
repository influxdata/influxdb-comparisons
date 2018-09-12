package bulk_query_gen

type CommonParams struct {
	AllInterval TimeInterval
	ScaleVar    int
}

func NewCommonParams(interval TimeInterval, scaleVar int) *CommonParams {
	return &CommonParams{
		AllInterval: interval,
		ScaleVar:    scaleVar,
	}
}
