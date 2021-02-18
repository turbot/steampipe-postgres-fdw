package hub

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// Next returns next row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)
	// Close stops an iteration and frees any resources.
	Close() error
}
