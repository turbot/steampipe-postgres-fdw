package cache

type QueryResult struct {
	Rows []map[string]interface{}
}

func (q *QueryResult) Append(row map[string]interface{}) {
	q.Rows = append(q.Rows, row)
}
