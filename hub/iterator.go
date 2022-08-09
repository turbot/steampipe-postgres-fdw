package hub

import "github.com/turbot/steampipe-plugin-sdk/v4/telemetry"

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// ConnectionName returns the connection name that this iterator uses.
	// for cacheIterators, this will be an empty string
	ConnectionName() string
	// Next returns next row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)
	// Close stops an iteration, ensures the plugin writes outstanding data to the cache and frees any resources.
	Close()
	// Abort stops an iteration,and frees any resources.
	Abort()
	Status() queryStatus
	Error() error
	CanIterate() bool
	GetScanMetadata() []ScanMetadata
	GetTraceContext() *telemetry.TraceCtx
}
