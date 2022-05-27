package hub

import (
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v3/instrument"

	"github.com/turbot/steampipe-postgres-fdw/hub/cache"
)

type cacheIterator struct {
	name     string
	rows     []map[string]interface{}
	index    int
	status   queryStatus
	traceCtx *instrument.TraceCtx
}

func newCacheIterator(name string, cachedResult *cache.QueryResult, traceCtx *instrument.TraceCtx) *cacheIterator {
	return &cacheIterator{
		name:     name,
		rows:     cachedResult.Rows,
		status:   QueryStatusReady,
		traceCtx: traceCtx,
	}
}

// ConnectionName implements Iterator
func (i *cacheIterator) ConnectionName() string {
	return i.name
}

func (i *cacheIterator) Status() queryStatus {
	return i.status
}

func (i *cacheIterator) Error() error {
	return nil
}

// Next implements Iterator
// return next row (tuple). Nil slice means there is no more rows to scan.
func (i *cacheIterator) Next() (map[string]interface{}, error) {
	if i.status == QueryStatusReady {
		i.status = QueryStatusStarted
	}

	if idx := i.index; idx < len(i.rows) {
		i.index++
		return i.rows[idx], nil
	}
	log.Printf("[TRACE] cacheIterator Next() complete (%p)", i)
	i.status = QueryStatusComplete
	return nil, nil
}

// Close implements Iterator
// clear the rows and the index
func (i *cacheIterator) Close(bool) {
	log.Printf("[TRACE] cacheIterator Close() (%p)", i)
	i.index = 0
	i.rows = nil
	i.status = QueryStatusReady
}

func (i *cacheIterator) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusComplete:
		return false
	default:
		return true
	}
}
