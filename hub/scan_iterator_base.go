package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"google.golang.org/protobuf/reflect/protoreflect"
	"log"
	"time"
)

type scanIteratorBase struct {
	status             queryStatus
	err                error
	rows               chan *proto.Row
	scanMetadata       map[string]*proto.QueryMetadata
	pluginRowStream    row_stream.Receiver
	rel                *types.Relation
	hub                Hub
	table              string
	connectionName     string
	connectionLimitMap map[string]int64
	cancel             context.CancelFunc
	traceCtx           *telemetry.TraceCtx
	queryContext       *proto.QueryContext

	startTime time.Time
	callId    string
}

func newBaseScanIterator(hub Hub, connectionName, table string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx) scanIteratorBase {
	return scanIteratorBase{
		status:             QueryStatusReady,
		rows:               make(chan *proto.Row, rowBufferSize),
		scanMetadata:       make(map[string]*proto.QueryMetadata),
		hub:                hub,
		table:              table,
		connectionName:     connectionName,
		connectionLimitMap: connectionLimitMap,
		traceCtx:           traceCtx,
		startTime:          time.Now(),
		queryContext:       proto.NewQueryContext(columns, qualMap, limit),
		callId:             grpc.BuildCallId(),
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
	log.Printf("[Trace] scanIteratorBase Next")
	// check the iterator state - has an error occurred
	if i.status == QueryStatusError {
		return nil, i.err
	}
	logging.LogTime("[hub] Next start")

	if !i.CanIterate() {
		// this is a bug
		log.Printf("[WARN] scanIteratorBase cannot iterate: connection %s, status: %s", i.GetConnectionName(), i.Status())
		return nil, fmt.Errorf("scanIteratorBase cannot iterate: connection %s, status: %s", i.GetConnectionName(), i.Status())
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

func (i *scanIteratorBase) closeSpan() {
	// if we have scan metadata, add to span
	// TODO SUM ALL metadata
	//if i.scanMetadata != nil {
	//	i.traceCtx.Span.SetAttributes(
	//		attribute.Int64("hydrate_calls", i.scanMetadata.HydrateCalls),
	//		attribute.Int64("rows_fetched", i.scanMetadata.RowsFetched),
	//		attribute.Bool("cache_hit", i.scanMetadata.CacheHit),
	//	)
	//}

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

// note: if this is an aggregator query, we will have a scan metadata for each connection
// we need to combine them into a single scan metadata object

func (i *scanIteratorBase) GetScanMetadata() ScanMetadata {
	res := ScanMetadata{
		Table:     i.table,
		Columns:   i.queryContext.Columns,
		Quals:     i.queryContext.Quals,
		StartTime: i.startTime,
		Duration:  time.Since(i.startTime),
	}
	for _, m := range i.scanMetadata {
		res.CacheHit = res.CacheHit || m.CacheHit
		res.RowsFetched += m.RowsFetched
		res.HydrateCalls += m.HydrateCalls
	}
	return res

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
			i.scanMetadata[rowResult.Connection] = rowResult.Metadata

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

// if there is an error other than EOF, save error and set state to QueryStatusError
func (i *scanIteratorBase) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}
