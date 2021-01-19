package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// Next returns next row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)

	// Reset restarts an iterator from the beginning (possible with a new data snapshot).
	Reset(columns []string, quals []*proto.Qual, opts types.Options)
	// Close stops an iteration and frees any resources.
	Close() error
}
