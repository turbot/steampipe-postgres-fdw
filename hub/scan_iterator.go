package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-postgres-fdw/hub/cache"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/steampipeconfig"
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
	status          queryStatus
	err             error
	rows            chan *proto.Row
	columns         []string
	limit           int64
	pluginRowStream proto.WrapperPlugin_ExecuteClient
	rel             *types.Relation
	qualMap         map[string]*proto.Quals
	hub             *Hub
	cachedRows      *cache.QueryResult
	cacheEnabled    bool
	cacheTTL        time.Duration
	table           string
	connection      *steampipeconfig.ConnectionPlugin
	cancel          context.CancelFunc
}

func newScanIterator(hub *Hub, connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals, columns []string, limit int64) *scanIterator {
	cacheEnabled := hub.cacheEnabled(connection.ConnectionName)
	cacheTTL := hub.cacheTTL(connection.ConnectionName)

	return &scanIterator{
		status:       QueryStatusReady,
		rows:         make(chan *proto.Row, rowBufferSize),
		hub:          hub,
		columns:      columns,
		limit:        limit,
		qualMap:      qualMap,
		cachedRows:   &cache.QueryResult{},
		cacheEnabled: cacheEnabled,
		cacheTTL:     cacheTTL,
		table:        table,
		connection:   connection,
	}
}

// access functions

func (i *scanIterator) ConnectionName() string {
	return i.connection.ConnectionName
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
		log.Printf("[TRACE] row channel is closed - reset iterator (%p)", i)

		// if iterator is in error, return the error
		if i.Status() == QueryStatusError {
			// return error
			return nil, i.err
		}
		log.Printf("[TRACE] scanIterator complete (%p)", i)
		// otherwise mark iterator complete, caching result
		i.status = QueryStatusComplete
		i.writeToCache()
	} else {
		// so we got a row
		var err error
		res, err = i.populateRow(row)
		if err != nil {
			return nil, err
		}
		// add row to cached rows
		if i.cacheEnabled {
			i.cachedRows.Append(res)
		}
	}

	logging.LogTime("[hub] Next end")
	return res, nil
}

func (i *scanIterator) Start(stream proto.WrapperPlugin_ExecuteClient, ctx context.Context, cancel context.CancelFunc) {
	logging.LogTime("[hub] start")

	log.Printf("[TRACE] scanIterator Start (%p)", i)
	i.status = QueryStatusStarted
	i.pluginRowStream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readThread(ctx)
}

func (i *scanIterator) Close(writeToCache bool) {
	log.Printf("[TRACE] scanIterator Close (%p)", i)

	// ping the cancel channel - if there is an active read thread, it will cancel the GRPC stream if needed
	log.Printf("[TRACE]  signalling cancel channel (%p)", i)
	// call the context cancellation function
	i.cancel()

	log.Printf("[TRACE] stream cancelled (%p)", i)
	if writeToCache {
		i.writeToCache()
	}
	// set status to complete
	if i.status != QueryStatusError {
		log.Printf("[TRACE] scanIterator complete (%p)", i)
		i.status = QueryStatusComplete
	}

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
	log.Printf("[TRACE] iterator readThread - read results from GRPC stream (%p)", i)
	// if the iterator is not in a started state, skip
	// (this can happen if postgres cancels the scan before receiving any results)
	if i.status == QueryStatusStarted {
		// keep calling readPluginResult until it returns false
		for i.readPluginResult(ctx) {
		}
	}

	// now we are done
	close(i.rows)
	log.Println("[TRACE] iterator readThread complete")
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
			log.Printf("[TRACE] readPluginResult nil row received - stop reading (%p)", i)
			// stop reading
			continueReading = false
		} else {
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

// called when all the data has been read from the stream
func (i *scanIterator) writeToCache() {
	log.Printf("[TRACE] writeToCache %s", i.ConnectionName())

	if !i.cacheEnabled {
		log.Printf("[TRACE] caching disabled - returning")
		// nothing to do
		return
	}

	res := i.hub.queryCache.Set(i.connection, i.table, i.qualMap, i.columns, i.limit, i.cachedRows, i.cacheTTL)

	if res {
		if len(i.cachedRows.Rows) > 0 {
			log.Printf("[INFO] adding %d rows to cache", len(i.cachedRows.Rows))
		}
	} else {
		log.Printf("[WARN] failed to add %d rows to cache", len(i.cachedRows.Rows))
	}
}

// if there is an error other than EOF, save error and set state to QueryStatusError
func (i *scanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}
