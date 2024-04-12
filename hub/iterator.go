package hub

import "github.com/turbot/steampipe-plugin-sdk/v5/telemetry"

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// ConnectionName returns the connection name that this iterator uses.
	// for cacheIterators, this will be an empty string
	ConnectionName() string
	// Next returns next row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)
	// Close stops an iteration and frees any resources.
	Close()
	Status() queryStatus
	Error() error
	CanIterate() bool
	GetScanMetadata() []ScanMetadata
	GetQueryTimestamp() int64
	GetTraceContext() *telemetry.TraceCtx
}
