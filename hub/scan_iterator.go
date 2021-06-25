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
	queryStatusReady    queryStatus = "ready"
	queryStatusStarted              = "started"
	queryStatusError                = "error"
	queryStatusComplete             = "complete"
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
	count        int
	cancel       context.CancelFunc
}

func newScanIterator(hub *Hub, connection *steampipeconfig.ConnectionPlugin, table string, qualMap map[string]*proto.Quals, columns []string, limit int64) *scanIterator {
	cacheEnabled := hub.cacheEnabled(connection.ConnectionName)
	cacheTTL := hub.cacheTTL(connection.ConnectionName)

	return &scanIterator{
		status:       queryStatusReady,
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
// return the next row (tuple). Nil slice means there is no more rows to scan.
func (i *scanIterator) Next() (map[string]interface{}, error) {

	i.count++
	logging.LogTime("[hub] Next start")

	if canIterate, err := i.CanIterate(); !canIterate {
		log.Printf("[WARN] scanIterator cannot iterate: %v \n", err)
		return nil, fmt.Errorf("cannot iterate: %v", err)
	}
	row := <-i.rows

	// now check the iterator state - has an error occurred
	if i.status == queryStatusError {
		return nil, i.err
	}

	// if the row channel closed, complete the iterator state
	var res map[string]interface{}
	if row == nil {
		log.Printf("[WARN] row channel is closed - reset iterator\n")
		i.onComplete()
		res = map[string]interface{}{}
	} else {
		res = make(map[string]interface{}, len(row.Columns))
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
	}

	// add row to cache
	if i.cacheEnabled {
		i.cachedRows.Append(res)
	}

	logging.LogTime("[hub] Next end")
	return res, nil
}

func (i *scanIterator) Close() {
	log.Println("[WARN] scanIterator Close *******************")
	if i.stream != nil {
		// close our GRPC stream from the plugin
		log.Printf("[WARN] there is a stream - calling KILL")

		i.stream.CloseSend()
		i.cancel()
		// clear iterator state, cache results (if enabled)
		i.onComplete()
	}
}

func (i *scanIterator) start(stream proto.WrapperPlugin_ExecuteClient, cancel context.CancelFunc) {
	logging.LogTime("[hub] start")
	if i.status != queryStatusReady {
		panic("attempting to start iterator which is still in progress")
	}
	i.status = queryStatusStarted
	i.stream = stream
	i.cancel = cancel

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readResults()
}

// read results from plugin stream, saving results in 'rows'.
// When we reach the end of the stream close the stram and the rows channel so consumers know there is know more data
func (i *scanIterator) readResults() {
	log.Printf("[DEBUG] readResults - read results from plugin stream, saving results in 'rows'\n")
	if i.status != queryStatusStarted {
		panic(fmt.Sprintf("attempting to read scan results but no iteration is in progress - iterator status %v", i.status))
	}

	for i.readResult() {
		time.Sleep(10 * time.Millisecond)
	}
}

// read a single result from the GRPC stream. Return true if there are more results to read
func (i *scanIterator) readResult() bool {
	// lock read lock to ensure the stream is not closed from under us by a call to close()
	i.readLock.Lock()
	defer i.readLock.Unlock()

	// if iterator has been closed, stream will be nil
	if i.stream == nil {
		log.Printf("[TRACE] scanIterator readResultstream is nil, it must have been closed")
		// stop reading
		return false
	}

	row, err := i.stream.Recv()

	if err != nil {
		if err.Error() != "EOF" {
			log.Printf("[WARN] stream receive error %v\n", err)
		}
		i.setError(err)
	}

	if row == nil {
		log.Printf("[WARN] nil row received - closing stream •••••••••**********\n")
		close(i.rows)
		i.stream.CloseSend()
		// stop reading
		return false
	} else {
		i.rows <- row.Row
	}
	// continue reading
	return true
}

// scanIterator state methods
func (i *scanIterator) inProgress() bool {
	return i.status == queryStatusStarted
}

func (i *scanIterator) failed() bool {
	return i.status == queryStatusError
}

// called when all the data has been read from the stream - complete status to queryStatusReady, and clear stream and error
func (i *scanIterator) onComplete() {
	log.Printf("[WARN] *********** onComplete %s", i.ConnectionName())
	// lock readlock so the stream read process does not try to read from the nil stream
	i.readLock.Lock()
	defer i.readLock.Unlock()

	i.status = queryStatusComplete
	i.stream = nil
	i.err = nil
	// write the data to the cache
	if i.cacheEnabled {
		res := i.hub.queryCache.Set(i.connection, i.table, i.qualMap, i.columns, i.limit, i.cachedRows, i.cacheTTL)
		log.Println("[INFO] scan complete")
		if res {
			log.Printf("[INFO] adding %d rows to cache", len(i.cachedRows.Rows))
		} else {
			log.Printf("[WARN] failed to add %d rows to cache", len(i.cachedRows.Rows))
		}
	}

}

// if there is an error other than EOF, save error and set state to queryStatusError
func (i *scanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = queryStatusError
		i.err = err
	}
}

// CanIterate :: return true if this iterator has results available to iterate
func (i *scanIterator) CanIterate() (bool, error) {
	switch i.status {
	case queryStatusError:
		return false, fmt.Errorf("there was an error executing scanIterator: %v", i.err)
	case queryStatusReady, queryStatusComplete:
		return false, fmt.Errorf("no scanIterator in progress")
	}
	return true, nil
}
