package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/logging"
	"github.com/turbot/steampipe-plugin-sdk/v4/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type legacyScanIterator struct {
	status           queryStatus
	err              error
	rows             chan *proto.Row
	scanMetadata     *proto.QueryMetadata
	columns          []string
	limit            int64
	pluginRowStream  proto.WrapperPlugin_ExecuteClient
	rel              *types.Relation
	qualMap          map[string]*proto.Quals
	hub              *Hub
	cacheEnabled     bool
	cacheTTL         time.Duration
	table            string
	connectionName   string
	connectionPlugin *steampipeconfig.ConnectionPlugin
	cancel           context.CancelFunc
	traceCtx         *telemetry.TraceCtx

	startTime time.Time
}

func newLegacyScanIterator(hub *Hub, connection *steampipeconfig.ConnectionPlugin, connectionName, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx) *legacyScanIterator {
	cacheTTL := hub.cacheTTL(connectionName)

	return &legacyScanIterator{
		status:           QueryStatusReady,
		rows:             make(chan *proto.Row, rowBufferSize),
		hub:              hub,
		columns:          columns,
		limit:            limit,
		qualMap:          qualMap,
		cacheTTL:         cacheTTL,
		table:            table,
		connectionName:   connectionName,
		connectionPlugin: connection,
		traceCtx:         traceCtx,
		startTime:        time.Now(),
	}
}

// access functions

func (i *legacyScanIterator) ConnectionName() string {
	return i.connectionName
}

func (i *legacyScanIterator) Status() queryStatus {
	return i.status
}

func (i *legacyScanIterator) Error() error {
	return i.err
}

// Next implements Iterator
// return the next row. Nil row means there are no more rows to scan.
func (i *legacyScanIterator) Next() (map[string]interface{}, error) {
	// check the iterator state - has an error occurred
	if i.status == QueryStatusError {
		return nil, i.err
	}
	logging.LogTime("[hub] Next start")

	if !i.CanIterate() {
		// this is a bug
		log.Printf("[WARN] legacyScanIterator cannot iterate: connection %s, status: %s", i.ConnectionName(), i.Status())
		return nil, fmt.Errorf("legacyScanIterator cannot iterate: connection %s, status: %s", i.ConnectionName(), i.Status())
	}

	row := <-i.rows

	// if the row channel closed, complete the iterator state
	var res map[string]interface{}
	if row == nil {
		// close the span
		i.closeSpan()

		// if iterator is in error, return the error
		if i.Status() == QueryStatusError {
			// return error
			return nil, i.err
		}
		// otherwise mark iterator complete, caching result
		i.status = QueryStatusComplete

	} else {

		// so we got a row
		var err error
		res, err = i.populateRow(row)
		if err != nil {
			return nil, err
		}
	}
	logging.LogTime("[hub] Next end")
	return res, nil
}

func (i *legacyScanIterator) closeSpan() {
	// if we have scan metadata, add to span
	if i.scanMetadata != nil {
		i.traceCtx.Span.SetAttributes(
			attribute.Int64("hydrate_calls", i.scanMetadata.HydrateCalls),
			attribute.Int64("rows_fetched", i.scanMetadata.RowsFetched),
			attribute.Bool("cache_hit", i.scanMetadata.CacheHit),
		)
	}

	i.traceCtx.Span.End()
}

func (i *legacyScanIterator) Start(stream proto.WrapperPlugin_ExecuteClient, ctx context.Context, cancel context.CancelFunc) {
	logging.LogTime("[hub] start")
	i.status = QueryStatusStarted
	i.pluginRowStream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readThread(ctx)
}

func (i *legacyScanIterator) Close() {
	// call the context cancellation function
	i.cancel()

	// set status to complete
	if i.status != QueryStatusError {
		i.status = QueryStatusComplete
	}

	i.closeSpan()
}

func (i *legacyScanIterator) Abort() {
	// for legacy iterator just call Close which has same behaviour as abort
	i.Close()
}

// CanIterate returns true if this iterator has results available to iterate
func (i *legacyScanIterator) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusReady, QueryStatusComplete:
		// scan iterator must be explicitly started - so we cannot iterate is in ready state
		return false
	default:
		return true
	}

}

func (i *legacyScanIterator) GetScanMetadata() []ScanMetadata {
	// scan metadata will only be populated for plugins using latest sdk
	if i.scanMetadata == nil {
		return nil
	}
	return []ScanMetadata{{
		Table:        i.table,
		CacheHit:     i.scanMetadata.CacheHit,
		RowsFetched:  i.scanMetadata.RowsFetched,
		HydrateCalls: i.scanMetadata.HydrateCalls,
		Columns:      i.columns,
		Quals:        i.qualMap,
		Limit:        i.limit,
		StartTime:    i.startTime,
		Duration:     time.Since(i.startTime),
	}}
}

func (i *legacyScanIterator) GetTraceContext() *telemetry.TraceCtx {
	return i.traceCtx
}

func (i *legacyScanIterator) populateRow(row *proto.Row) (map[string]interface{}, error) {
	res := make(map[string]interface{}, len(row.Columns))
	for columnName, column := range row.Columns {
		// extract column value as interface from protobuf message
		var val interface{}
		if bytes := column.GetJsonValue(); bytes != nil {
			if err := json.Unmarshal(bytes, &val); err != nil {
				err = fmt.Errorf("failed to populate column '%s': %v", columnName, err)
				i.setError(err)
				return nil, err
			}
		} else if timestamp := column.GetTimestampValue(); timestamp != nil {
			// convert from protobuf timestamp to a RFC 3339 time string
			val = ptypes.TimestampString(timestamp)
		} else {
			// get the first field descriptor and value (we only expect column message to contain a single field
			column.ProtoReflect().Range(func(descriptor protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				// is this value null?
				if descriptor.JSONName() == "nullValue" {
					val = nil
				} else {
					val = v.Interface()
				}
				return false
			})
		}
		res[columnName] = val
	}
	return res, nil
}

// readThread is run in a goroutine and reads results from the GRPC stream until either:
// - the stream is complete
// - there stream returns an error
// there is a signal on the cancel channel
func (i *legacyScanIterator) readThread(ctx context.Context) {
	// if the iterator is not in a started state, skip
	// (this can happen if postgres cancels the scan before receiving any results)
	if i.status == QueryStatusStarted {
		// keep calling readPluginResult until it returns false
		for i.readPluginResult(ctx) {
		}
	}

	// now we are done
	close(i.rows)
}

func (i *legacyScanIterator) readPluginResult(ctx context.Context) bool {
	continueReading := true
	var rcvChan = make(chan *proto.ExecuteResponse)
	var errChan = make(chan error)

	// put the stream receive code into a goroutine to ensure cancellation is possible in case of a plugin hang
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- helpers.ToError(r)
			}
		}()
		rowResult, err := i.pluginRowStream.Recv()

		if err != nil {
			errChan <- err
		} else {
			rcvChan <- rowResult
		}
	}()

	select {
	// check for cancellation first - this takes precedence over reading the grpc stream
	case <-ctx.Done():
		log.Printf("[TRACE] readPluginResult context is cancelled (%p)", i)
		continueReading = false
	case rowResult := <-rcvChan:
		if rowResult == nil {
			log.Printf("[TRACE] readPluginResult nil row received - stop reading (%p)", i)
			// stop reading
			continueReading = false
		} else {
			// update the scan metadata (this will overwrite any existing from the previous row)
			i.scanMetadata = rowResult.Metadata

			// so we have a row
			i.rows <- rowResult.Row
		}
	case err := <-errChan:
		if err.Error() == "EOF" {
			log.Printf("[TRACE] readPluginResult EOF error received - stop reading (%p)", i)
		} else {
			log.Printf("[WARN] stream receive error %v (%p)\n", err, i)
			i.setError(err)
		}
		// stop reading
		continueReading = false
	}
	return continueReading
}

// if there is an error other than EOF, save error and set state to QueryStatusError
func (i *legacyScanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}
