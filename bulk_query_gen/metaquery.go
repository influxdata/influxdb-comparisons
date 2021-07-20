package bulk_query_gen

type Metaquery interface {
	StandardMetaquery(Query)
	Dispatch(int) Query
}

func MetaqueryDispatchAll(d Metaquery, q Query) {
	d.StandardMetaquery(q)
}
