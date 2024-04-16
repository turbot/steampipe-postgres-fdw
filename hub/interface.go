package hub

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe/pkg/query/queryresult"
)

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// GetConnectionName returns the connection name that this iterator uses.
	GetConnectionName() string
	GetPluginName() string
	// Next returns a row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)
	// Close stops an iteration and frees any resources.
	Close()
	Status() queryStatus
	Error() error
	CanIterate() bool
	GetQueryTimestamp() int64
	GetTraceContext() *telemetry.TraceCtx
}

type pluginExecutor interface {
	execute(req *proto.ExecuteRequest) (str row_stream.Receiver, ctx context.Context, cancel context.CancelFunc, err error)
}

type pluginIterator interface {
	Iterator
	GetQueryContext() *proto.QueryContext
	GetCallId() string
	GetConnectionLimitMap() map[string]int64
	SetError(err error)
	GetTable() string
	GetScanMetadata() []queryresult.ScanMetadataRow
	Start(pluginExecutor) error
}
