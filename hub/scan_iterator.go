package hub

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/turbot/steampipe-postgres-fdw/hub/cache"

	"github.com/golang/protobuf/ptypes"
	typeHelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type queryStatus string

const (
	querystatusNone    queryStatus = "none"
	querystatusStarted             = "started"
	//queryStatus_finished             = "finished"
	querystatusError = "error"
)

type scanIterator struct {
	status     queryStatus
	err        error
	rows       chan *proto.Row
	columns    []string
	stream     proto.WrapperPlugin_ExecuteClient
	rel        *types.Relation
	qualMap    map[string]*proto.Quals
	hub        *Hub
	cachedRows *cache.QueryResult
	opts       types.Options
}

func newScanIterator(hub *Hub, rel *types.Relation, columns []string, qualMap map[string]*proto.Quals, opts types.Options) *scanIterator {

	return &scanIterator{
		status:     querystatusNone,
		rows:       make(chan *proto.Row, rowBufferSize),
		hub:        hub,
		rel:        rel,
		columns:    columns,
		qualMap:    qualMap,
		cachedRows: &cache.QueryResult{},
		opts:       opts,
	}
}

// Iterator implementation
// Next returns next row (tuple). Nil slice means there is no more rows to scan.
func (i *scanIterator) Next() (map[string]interface{}, error) {
	logging.LogTime("[hub] Next start")

	if canIterate, err := i.CanIterate(); !canIterate {
		log.Printf("[WARN] scanIterator cannot iterate: %v \n", err)
		return nil, fmt.Errorf("cannot iterate: %v", err)
	}
	row := <-i.rows

	// now check the iterator state - has an error occurred
	if i.status == querystatusError {
		return nil, i.err
	}

	//log.Printf("[DEBUG] row %v  \n", row)

	// if the row channel closed, complete the iterator state
	var res map[string]interface{}
	if row == nil {
		log.Printf("[DEBUG] row channel is closed - reset iterator\n")
		i.onComplete()
		res = map[string]interface{}{}
	} else {
		res = make(map[string]interface{}, len(row.Columns))
		for columnName, column := range row.Columns {
			// extract column value as interface from protobuf message
			var val interface{}
			if bytes := column.GetJsonValue(); bytes != nil {
				if err := json.Unmarshal(bytes, &val); err != nil {
					return nil, fmt.Errorf("failed to populate column '%s': %v", columnName, err)
				}
			} else if timestamp := column.GetTimestampValue(); timestamp != nil {
				// convert from protobuf timestamp to time.Time
				timeString := ptypes.TimestampString(timestamp)

				var err error
				if val, err = typeHelpers.ToTime(timeString); err != nil {
					return nil, fmt.Errorf("scanIterator failed to populate %s column: %v", columnName, err)
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
	if i.hub.cachingEnabled {
		i.cachedRows.Append(res)
	}

	logging.LogTime("[hub] Next end")
	return res, nil
}

func (i *scanIterator) Close() error {
	// how to close?
	return nil
}

func (i *scanIterator) start(stream proto.WrapperPlugin_ExecuteClient) {
	logging.LogTime("[hub] start")
	if i.status != querystatusNone {
		panic("attempting to start iterator which is still in progress")
	}
	i.status = querystatusStarted
	i.stream = stream

	// read the results - this will loop until it hits an error or the stream is closed
	go i.readResults()
}

// read results from plugin stream, saving results in 'rows'.
// When we reach the end of the stream close the stram and the rows channel so consumers know there is know more data
func (i *scanIterator) readResults() {
	log.Printf("[DEBUG] readResults - read results from plugin stream, saving results in 'rows'\n")
	if i.status != querystatusStarted {
		panic(fmt.Sprintf("attempting to read scan results but no iteration is in progress - iterator status %v", i.status))
	}

	for {
		row, err := i.stream.Recv()
		logging.LogTime("[hub] receive complete")
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("[WARN] stream receive error %v\n", err)
			}
			i.setError(err)
		}
		if row == nil {
			log.Printf("[DEBUG] nil row received - closing stream\n")
			close(i.rows)
			i.stream.CloseSend()
			return
		} else {
			i.rows <- row.Row
		}
	}
}

// scanIterator state methods
func (i *scanIterator) inProgress() bool {
	return i.status == querystatusStarted
}

func (i *scanIterator) failed() bool {
	return i.status == querystatusError
}

// called when all the data has been read from the stream - complete status to querystatusNone, and clear stream and error
func (i *scanIterator) onComplete() {

	i.status = querystatusNone
	i.stream = nil
	i.err = nil
	// write the data to the cache
	table := i.opts["table"]

	if i.hub.cachingEnabled {
		log.Printf("[INFO] Scan complete - adding %d rows to cache", len(i.cachedRows.Rows))
		i.hub.queryCache.Set(table, i.qualMap, i.columns, i.cachedRows)
	}

}

// if there is an error other than EOF, save error and set state to querystatusError
func (i *scanIterator) setError(err error) {
	if err != nil && err.Error() != "EOF" {
		i.status = querystatusError
		i.err = err
	}
}

// CanIterate :: return true if this iterator has results available to iterate
func (i *scanIterator) CanIterate() (bool, error) {
	if i.status == querystatusError {
		return false, fmt.Errorf("there was an error executing scanIterator: %v", i.err)
	}
	if i.status == querystatusNone {
		return false, fmt.Errorf("no scanIterator in progress")
	}
	return true, nil
}
