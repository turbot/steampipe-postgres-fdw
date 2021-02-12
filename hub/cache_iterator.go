package hub

type cacheIterator struct {
	rows  []map[string]interface{}
	index int
}

func newCacheIterator(rows []map[string]interface{}) *cacheIterator {
	return &cacheIterator{
		rows: rows,
	}
}

// Iterator implementation
// Next returns next row (tuple). Nil slice means there is no more rows to scan.
func (i *cacheIterator) Next() (map[string]interface{}, error) {
	if idx := i.index; idx < len(i.rows) {
		i.index++
		return i.rows[idx], nil

	}
	// no more cached rows
	return nil, nil
}

// Close :: clear the rows and the index
func (i *cacheIterator) Close() error {
	i.index = 0
	i.rows = nil
	return nil
}
