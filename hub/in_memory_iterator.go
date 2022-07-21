package hub

import (
	"context"
	"log"

	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
)

type inMemoryIterator struct {
	name   string
	rows   []map[string]interface{}
	index  int
	status queryStatus
}

func newInMemoryIterator(name string, result *QueryResult) *inMemoryIterator {
	return &inMemoryIterator{
		name:   name,
		rows:   result.Rows,
		status: QueryStatusReady,
	}
}

// ConnectionName implements Iterator
func (i *inMemoryIterator) ConnectionName() string {
	return i.name
}

func (i *inMemoryIterator) Status() queryStatus {
	return i.status
}

func (i *inMemoryIterator) Error() error {
	return nil
}

// Next implements Iterator
// return next row (tuple). Nil slice means there is no more rows to scan.
func (i *inMemoryIterator) Next() (map[string]interface{}, error) {
	if i.status == QueryStatusReady {
		i.status = QueryStatusStarted
	}

	if idx := i.index; idx < len(i.rows) {
		i.index++
		return i.rows[idx], nil
	}
	log.Printf("[TRACE] inMemoryIterator Next() complete (%p)", i)
	i.status = QueryStatusComplete
	return nil, nil
}

// Close implements Iterator
// clear the rows and the index
func (i *inMemoryIterator) Close() {
	log.Printf("[TRACE] inMemoryIterator Close() (%p)", i)
	i.index = 0
	i.rows = nil
	i.status = QueryStatusReady
}

func (i *inMemoryIterator) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusComplete:
		return false
	default:
		return true
	}
}

func (i *inMemoryIterator) GetScanMetadata() []ScanMetadata {
	return nil
}
func (i *inMemoryIterator) GetTraceContext() *telemetry.TraceCtx {
	return &telemetry.TraceCtx{Ctx: context.Background()}
}
