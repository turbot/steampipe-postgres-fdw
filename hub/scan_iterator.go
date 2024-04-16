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
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/query/queryresult"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TODO think about when we reset status from complete to ready

type queryStatus string

const (
	QueryStatusReady    queryStatus = "ready"
	QueryStatusStarted  queryStatus = "started"
	QueryStatusError    queryStatus = "error"
	QueryStatusComplete queryStatus = "complete"
)

type scanIterator struct {
	status             queryStatus
	err                error
	rows               chan *proto.Row
	scanMetadata       map[string]*proto.QueryMetadata
	pluginRowStream    proto.WrapperPlugin_ExecuteClient
	rel                *types.Relation
	hub                *Hub
	table              string
	connectionName     string
	connectionLimitMap map[string]int64
	connectionPlugin   *steampipeconfig.ConnectionPlugin
	cancel             context.CancelFunc
	traceCtx           *telemetry.TraceCtx
	queryContext       *proto.QueryContext
	// the query timestamp is used to uniquely identify the parent query
	// NOTE: all scans for the query will have the same timestamp
	queryTimestamp int64

	startTime time.Time
	callId    string
}

func newScanIterator(hub *Hub, connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName, table string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx, queryTimestamp int64) *scanIterator {
	return &scanIterator{
		status:             QueryStatusReady,
		rows:               make(chan *proto.Row, rowBufferSize),
		scanMetadata:       make(map[string]*proto.QueryMetadata),
		hub:                hub,
		table:              table,
		connectionName:     connectionName,
		connectionLimitMap: connectionLimitMap,
		connectionPlugin:   connectionPlugin,
		traceCtx:           traceCtx,
		startTime:          time.Now(),
		queryContext:       proto.NewQueryContext(columns, qualMap, limit),
		callId:             grpc.BuildCallId(),
		queryTimestamp:     queryTimestamp,
	}
}

// access functions

func (i *scanIterator) ConnectionName() string {
	return i.connectionName
}

func (i *scanIterator) Status() queryStatus {
	return i.status
}

func (i *scanIterator) Error() error {
	return i.err
}

// Next implements Iterator
// return the next row. Nil row means there are no more rows to scan.
func (i *scanIterator) Next() (map[string]interface{}, error) {
	// check the iterator state - has an error occurred
	if i.status == QueryStatusError {
		return nil, i.err
	}
	logging.LogTime("[hub] Next start")

	if !i.CanIterate() {
		// this is a bug
		log.Printf("[WARN] scanIterator cannot iterate: connection %s, status: %s", i.ConnectionName(), i.Status())
		return nil, fmt.Errorf("scanIterator cannot iterate: connection %s, status: %s", i.ConnectionName(), i.Status())
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

func (i *scanIterator) closeSpan() {
	i.traceCtx.Span.End()
}

func (i *scanIterator) Start(stream proto.WrapperPlugin_ExecuteClient, ctx context.Context, cancel context.CancelFunc) {
	logging.LogTime("[hub] start")
	log.Printf("[INFO] scanIterator Start (%p) (%s)", i, i.callId)
	i.status = QueryStatusStarted
	i.pluginRowStream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readThread(ctx)
}

func (i *scanIterator) Close() {
	log.Printf("[INFO] scanIterator Close (%p) (%s)", i, i.callId)
	// call the context cancellation function
	i.cancel()

	// set status to complete
	if i.status != QueryStatusError {
		i.status = QueryStatusComplete
	}

	i.closeSpan()
}

// CanIterate returns true if this iterator has results available to iterate
func (i *scanIterator) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusReady, QueryStatusComplete:
		// scan iterator must be explicitly started - so we cannot iterate is in ready state
		return false
	default:
		return true
	}

}

// GetScanMetadata returns the scan metadata for this iterator
// note: if this is an aggregator query, we will have a scan metadata for each connection
// we need to combine them into a single scan metadata object
func (i *scanIterator) GetScanMetadata() []queryresult.ScanMetadataRow {
	log.Printf("[INFO] scanIterator GetScanMetadata (%p) (%s)", i, i.callId)
	defer log.Printf("[INFO] scanIterator GetScanMetadata end (%p) (%s)", i, i.callId)
	var res = make([]queryresult.ScanMetadataRow, 0, len(i.scanMetadata))

	for connection, m := range i.scanMetadata {
		res = append(res, i.newScanMetadata(connection, m))
	}
	// if there is no scan metadata, add an empty one
	if len(res) == 0 {
		for connection := range i.connectionLimitMap {
			res = append(res, i.newScanMetadata(connection, nil))
		}
	}
	return res

}

func (i *scanIterator) GetQueryTimestamp() int64 {
	return i.queryTimestamp
}

func (i *scanIterator) newScanMetadata(connection string, m *proto.QueryMetadata) queryresult.ScanMetadataRow {
	res := queryresult.NewScanMetadataRow(connection, i.table, i.queryContext.Columns, i.queryContext.Quals, i.startTime, time.Since(i.startTime), i.connectionLimitMap[connection], m)

	return res
}

func (i *scanIterator) GetTraceContext() *telemetry.TraceCtx {
	return i.traceCtx
}

func (i *scanIterator) populateRow(row *proto.Row) (map[string]interface{}, error) {
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
func (i *scanIterator) readThread(ctx context.Context) {
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

func (i *scanIterator) readPluginResult(ctx context.Context) bool {
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
func (i *scanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}
