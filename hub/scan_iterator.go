package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	typeHelpers "github.com/turbot/go-kit/types"
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
	status       queryStatus
	err          error
	rows         chan *proto.Row
	columns      []string
	limit        int64
	stream       proto.WrapperPlugin_ExecuteClient
	rel          *types.Relation
	qualMap      map[string]*proto.Quals
	hub          *Hub
	cachedRows   *cache.QueryResult
	cacheEnabled bool
	cacheTTL     time.Duration
	table        string
	connection   *steampipeconfig.ConnectionPlugin
	readLock     sync.Mutex
	cancel       context.CancelFunc
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
		log.Printf("[WARN] row channel is closed - reset iterator\n")
		// if iterator is in error, return the error
		if i.Status() == QueryStatusError {
			return nil, i.err
		}
		// otherwise mark iterator complete, caching result
		i.onComplete(true)
	} else {
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
			// convert from protobuf timestamp to time.Time
			timeString := ptypes.TimestampString(timestamp)

			var err error
			if val, err = typeHelpers.ToTime(timeString); err != nil {
				err = fmt.Errorf("scanIterator failed to populate %s column: %v", columnName, err)
				i.setError(err)
				return nil, err
			}
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

func (i *scanIterator) Start(stream proto.WrapperPlugin_ExecuteClient, cancel context.CancelFunc) {
	logging.LogTime("[hub] start")
	if i.status != QueryStatusReady {
		panic("attempting to start iterator which is still in progress")
	}
	i.status = QueryStatusStarted
	i.stream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readResults()
}

func (i *scanIterator) Close(writeToCache bool) {

	// lock readlock so the stream read process does not try to read from the nil stream
	i.readLock.Lock()
	defer i.readLock.Unlock()

	log.Println("[TRACE] scanIterator Close")

	// if there is an active stream, cancel it
	if i.stream != nil {
		// close our GRPC stream from the plugin
		log.Printf("[WARN] there is a stream - calling cancel")

		i.stream.CloseSend()
		i.cancel()
		close(i.rows)
		// clear iterator state, cache results (if enabled)
		i.onComplete(writeToCache)
	}
}

// read results from plugin stream, saving results in 'rows'.
// When we reach the end of the stream close the stram and the rows channel so consumers know there is know more data
func (i *scanIterator) readResults() {
	log.Printf("[TRACE] readResults - read results from plugin stream, saving results in 'rows'\n")
	if i.status != QueryStatusStarted {
		panic(fmt.Sprintf("attempting to read scan results but no iteration is in progress - iterator status %v", i.status))
	}

	for i.readResult() {
		time.Sleep(10 * time.Millisecond)
	}
}

// read a single result from the GRPC stream. Return true if there are more results to read
func (i *scanIterator) readResult() bool {
	var row *proto.Row
	continueReading := true

	// lock read lock to ensure the stream is not closed from under us by a call to close()
	i.readLock.Lock()
	defer i.readLock.Unlock()

	// if iterator has been closed, stream will be nil
	if i.stream == nil {
		log.Printf("[TRACE] scanIterator readResultstream is nil, it must have been closed")
		// stop reading
		return false
	}

	rowResult, err := i.stream.Recv()

	if err != nil {
		if err.Error() != "EOF" {
			log.Printf("[WARN] stream receive error %v\n", err)
		}
		i.setError(err)
		// stop reading
		continueReading = false
	}

	if rowResult == nil {
		log.Printf("[WARN] nil row received - closing stream\n")
		// stop reading
		continueReading = false
	} else {
		// so we have a row
		row = rowResult.Row
	}

	// send row (which may be nil)
	i.rows <- row

	// continue reading
	return continueReading
}

// scanIterator state methods
func (i *scanIterator) inProgress() bool {
	return i.status == QueryStatusStarted
}

func (i *scanIterator) failed() bool {
	return i.status == QueryStatusError
}

// called when all the data has been read from the stream - complete status to QueryStatusReady, and clear stream and error
func (i *scanIterator) onComplete(writeToCache bool) {
	log.Printf("[WARN] onComplete %s, writeToCache %v", i.ConnectionName(), writeToCache)
	// lock readlock so the stream read process does not try to read from the nil stream
	i.readLock.Lock()
	defer i.readLock.Unlock()

	if i.status == QueryStatusReady {
		// nothing to do
		return
	}

	// TODO do we need complete status or can I reset to read
	i.status = QueryStatusComplete
	i.stream = nil
	i.err = nil
	// write the data to the cache
	if i.cacheEnabled && writeToCache {
		res := i.hub.queryCache.Set(i.connection, i.table, i.qualMap, i.columns, i.limit, i.cachedRows, i.cacheTTL)
		log.Println("[INFO] scan complete")
		if res {
			log.Printf("[INFO] adding %d rows to cache", len(i.cachedRows.Rows))
		} else {
			log.Printf("[WARN] failed to add %d rows to cache", len(i.cachedRows.Rows))
		}
	}

}

// if there is an error other than EOF, save error and set state to QueryStatusError
func (i *scanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = QueryStatusError
		i.err = err
	}
}

// CanIterate :: return true if this iterator has results available to iterate
func (i *scanIterator) CanIterate() bool {
	switch i.status {
	case QueryStatusError, QueryStatusReady, QueryStatusComplete:
		// scan iterator must be explicitly started - so we cannot iterate is in ready state
		return false
	default:
		return true
	}

}
