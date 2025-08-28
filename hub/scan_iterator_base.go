package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/v2/types"
	"github.com/turbot/steampipe/v2/pkg/query/queryresult"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type scanIteratorBase struct {
	status             queryStatus
	err                error
	rows               chan *proto.Row
	scanMetadata       map[string]queryresult.ScanMetadataRow
	pluginRowStream    row_stream.Receiver
	rel                *types.Relation
	hub                Hub
	table              string
	connectionName     string
	connectionLimitMap map[string]int64
	cancel             context.CancelFunc
	traceCtx           *telemetry.TraceCtx
	queryContext       *proto.QueryContext
	// the query timestamp is used to uniquely identify the parent query
	// NOTE: all scans for the query will have the same timestamp
	queryTimestamp int64

	startTime time.Time
	callId    string
}

func newBaseScanIterator(hub Hub, connectionName, table string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, sortOrder []*proto.SortColumn, queryTimestamp int64, traceCtx *telemetry.TraceCtx) scanIteratorBase {
	return scanIteratorBase{
		status:             QueryStatusReady,
		rows:               make(chan *proto.Row, rowBufferSize),
		scanMetadata:       make(map[string]queryresult.ScanMetadataRow),
		hub:                hub,
		table:              table,
		connectionName:     connectionName,
		connectionLimitMap: connectionLimitMap,
		traceCtx:           traceCtx,
		startTime:          time.Now(),
		queryContext:       proto.NewQueryContext(columns, qualMap, limit, sortOrder),
		callId:             grpc.BuildCallId(),
		queryTimestamp:     queryTimestamp,
	}
}

// access functions

func (i *scanIteratorBase) GetConnectionName() string {
	return i.connectionName
}

func (i *scanIteratorBase) Status() queryStatus {
	return i.status
}

func (i *scanIteratorBase) Error() error {
	return i.err
}

// Next implements Iterator
// return the next row. Nil row means there are no more rows to scan.
func (i *scanIteratorBase) Next() (map[string]interface{}, error) {
	// check the iterator state - has an error occurred
	if i.status == QueryStatusError {
		return nil, i.err
	}

	if !i.CanIterate() {
		// this is a bug
		log.Printf("[WARN] scanIteratorBase cannot iterate: connection %s, status: %s", i.GetConnectionName(), i.Status())
		return nil, fmt.Errorf("scanIteratorBase cannot iterate: connection %s, status: %s", i.GetConnectionName(), i.Status())
	}

	row := <-i.rows

	// if the row channel closed, complete the iterator state
	var res map[string]interface{}
	if row == nil {
		// close the span and set the status
		i.Close()
		// remove from hub running iterators
		i.hub.RemoveIterator(i)

		// if iterator is in error, return the error
		if i.Status() == QueryStatusError {
			// return error
			return nil, i.err
		}
		// otherwise mark iterator complete, caching result
	} else {
		// so we got a row
		var err error
		res, err = i.populateRow(row)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (i *scanIteratorBase) closeSpan() {
	i.traceCtx.Span.End()
}

func (i *scanIteratorBase) Close() {
	// call the context cancellation function
	i.cancel()

	// set status to complete
	if i.status != QueryStatusError {
		i.status = QueryStatusComplete
	}

	i.closeSpan()
}

// CanIterate returns true if this iterator has results available to iterate
func (i *scanIteratorBase) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusReady, QueryStatusComplete:
		// scan iterator must be explicitly started - so we cannot iterate is in ready state
		return false
	default:
		return true
	}

}

func (i *scanIteratorBase) GetTraceContext() *telemetry.TraceCtx {
	return i.traceCtx
}

func (i *scanIteratorBase) GetQueryContext() *proto.QueryContext {
	return i.queryContext
}

func (i *scanIteratorBase) GetCallId() string {
	return i.callId
}

func (i *scanIteratorBase) GetConnectionLimitMap() map[string]int64 {
	return i.connectionLimitMap
}

func (i *scanIteratorBase) SetError(err error) {
	i.err = err
}

func (i *scanIteratorBase) GetTable() string {
	return i.table
}

func (i *scanIteratorBase) Start(executor pluginExecutor) error {
	req := i.newExecuteRequest()

	// create context anc cancel function

	stream, ctx, cancel, err := executor.execute(req)
	if err != nil {
		return err
	}

	logging.LogTime("[hub] start")
	i.status = QueryStatusStarted
	i.pluginRowStream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readThread(ctx)
	return nil
}

// GetPluginName implements Iterator (this should be implemented by the concrete iterator)
func (i *scanIteratorBase) GetPluginName() string {
	panic("method GetPluginName not implemented")
}

func (i *scanIteratorBase) GetQueryTimestamp() int64 {
	return i.queryTimestamp
}

func (i *scanIteratorBase) GetScanMetadata() []queryresult.ScanMetadataRow {
	return nil
}

func (i *scanIteratorBase) newExecuteRequest() *proto.ExecuteRequest {
	req := &proto.ExecuteRequest{
		Table:        i.table,
		QueryContext: i.queryContext,
		CallId:       i.callId,
		// pass connection name - used for aggregators
		Connection:            i.connectionName,
		TraceContext:          grpc.CreateCarrierFromContext(i.traceCtx.Ctx),
		ExecuteConnectionData: make(map[string]*proto.ExecuteConnectionData),
	}

	log.Printf("[INFO] build executeConnectionData map: hub: %p, i.connectionLimitMap: %v", i.hub, i.connectionLimitMap)
	// build executeConnectionData map
	for connectionName, limit := range i.connectionLimitMap {
		data := &proto.ExecuteConnectionData{}
		if limit != -1 {
			data.Limit = &proto.NullableInt{Value: limit}
		}
		data.CacheTtl = int64(i.hub.cacheTTL(connectionName).Seconds())
		data.CacheEnabled = i.hub.cacheEnabled(connectionName)
		req.ExecuteConnectionData[connectionName] = data

	}
	log.Printf("[INFO] build executeConnectionData map returning %v", req)

	return req
}
func (i *scanIteratorBase) populateRow(row *proto.Row) (map[string]interface{}, error) {
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
			log.Printf("[INFO] populateRow >> JSON value - column: %q, val: %#v, raw_bytes: %q", columnName, val, bytes)
		} else if timestamp := column.GetTimestampValue(); timestamp != nil {
			// convert from protobuf timestamp to a RFC 3339 time string
			val = ptypes.TimestampString(timestamp)
			log.Printf("[INFO] populateRow >> Timestamp value - column: %q, val: %q, raw_bytes: %q", columnName, val, bytes)
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
			log.Printf("[INFO] populateRow >> Fallback value - column: %q, val: %#v, raw_bytes: %q", columnName, val, bytes)
		}
		res[columnName] = val
	}
	return res, nil
}

// readThread is run in a goroutine and reads results from the GRPC stream until either:
// - the stream is complete
// - there stream returns an error
// there is a signal on the cancel channel
func (i *scanIteratorBase) readThread(ctx context.Context) {
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

func (i *scanIteratorBase) readPluginResult(ctx context.Context) bool {
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
			log.Printf("[TRACE] readPluginResult nil row received - stop reading (%p) (%s)", i, i.callId)
			// stop reading
			continueReading = false
		} else {
			// update the scan metadata for this connection (this will overwrite any existing from the previous row)
			i.scanMetadata[rowResult.Connection] = i.newScanMetadata(rowResult.Connection, rowResult.Metadata)

			log.Printf("[INFO] readPluginResult rowResult: %q", rowResult.Row)
			// so we have a row
			i.rows <- rowResult.Row
		}
	case err := <-errChan:
		if err.Error() == "EOF" {
			log.Printf("[TRACE] readPluginResult EOF error received - stop reading (%p) (%s)", i, i.callId)
		} else {
			log.Printf("[WARN] stream receive error %v (%p)\n", err, i)
			i.setError(err)
		}
		// stop reading
		continueReading = false
	}
	return continueReading
}

func (i *scanIteratorBase) newScanMetadata(connection string, m *proto.QueryMetadata) queryresult.ScanMetadataRow {
	return queryresult.NewScanMetadataRow(connection, i.table, i.queryContext.Columns, i.queryContext.Quals, i.startTime, time.Since(i.startTime), i.connectionLimitMap[connection], m)
}

// if there is an error other than EOF, save error and set state to QueryStatusError
func (i *scanIteratorBase) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}
