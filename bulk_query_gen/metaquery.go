package bulk_query_gen

type Metaquery interface {
	StandardMetaquery(Query)
	Dispatch(int) Query
}

func MetaqueryDispatchAll(d Metaquery, iteration int, q Query, scaleVar int) {
	d.StandardMetaquery(q)
}
