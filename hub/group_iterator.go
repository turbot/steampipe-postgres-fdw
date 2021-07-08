package hub

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/steampipe/utils"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe/steampipeconfig/modconfig"
)

// groupIterator is a struct which aggregates the results of as number of interators
type groupIterator struct {
	Name             string
	Iterators        []Iterator
	rowChan          chan map[string]interface{}
	rowLock          sync.Mutex
	iteratorsRunning int
}

func NewGroupIterator(name string, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, connectionMap map[string]*modconfig.Connection, h *Hub) (Iterator, error) {
	res := &groupIterator{
		Name: name,
		// create a buffered channel
		rowChan: make(chan map[string]interface{}, rowBufferSize),
	}
	var errors []error
	for connectionName := range connectionMap {
		iterator, err := h.startScanForConnection(connectionName, table, qualMap, columns, limit)
		if err != nil {
			errors = append(errors, err)
		} else {
			res.Iterators = append(res.Iterators, iterator)
		}
	}
	if len(errors) > 0 {
		return nil, helpers.CombineErrors(errors...)
	}

	// now start iterating our children
	res.start()

	return res, nil
}

func (i *groupIterator) ConnectionName() string {
	return i.Name
}

func (i *groupIterator) Status() queryStatus {
	status := QueryStatusComplete
	for _, it := range i.Iterators {
		switch it.Status() {
		case QueryStatusError:
			// if any iterator is in error, we are in error
			return QueryStatusError

		case QueryStatusStarted:
			// if any iterator is in progress, we are in progress (unless we are in error)
			status = QueryStatusStarted
		case QueryStatusReady:
			// if we think we are complete, actually we are not
			if status == QueryStatusComplete {
				status = QueryStatusReady
			}
		}
	}
	return status
}

func (i *groupIterator) Error() error {
	return nil
}

// Next implements Iterator
func (i *groupIterator) Next() (map[string]interface{}, error) {
	row := <-i.rowChan
	if len(row) == 0 {
		return nil, i.aggregateIteratorErrors()
	}
	return row, nil

}

func (i *groupIterator) start() {
	// start a goroutine for each iterator
	for _, child := range i.Iterators {
		log.Printf("[TRACE] stream results from connection %s", child.ConnectionName())
		// increment th running count
		i.iteratorsRunning++
		go i.streamIteratorResults(child)
	}

}

func (i *groupIterator) streamIteratorResults(child Iterator) {
	log.Printf("[TRACE] streamIteratorResults connection %s", child.ConnectionName())
	for {
		// call next - ignore error ass the iterator state will store it
		row, _ := child.Next()
		log.Printf("[WARN] streamIteratorResults connection %s got row", child.ConnectionName())
		// if no row was returned, we are done
		if len(row) == 0 {
			log.Printf("[WARN] streamIteratorResults connection %s empty row", child.ConnectionName())
			i.rowLock.Lock()
			// decrement the running count
			i.iteratorsRunning--
			// we we were the last one, send an empty row
			if i.iteratorsRunning == 0 {
				log.Printf("[WARN] streamIteratorResults connection %s last iterator - stream empty row", child.ConnectionName())
				i.rowChan <- row

			}
			i.rowLock.Unlock()
			return
		}
		// lock access to row chan and stream row
		i.rowLock.Lock()
		log.Printf("[WARN] streamIteratorResults connection %s stream row", child.ConnectionName())
		i.rowChan <- row
		log.Printf("[WARN] streamIteratorResults connection %s streamed row", child.ConnectionName())
		i.rowLock.Unlock()
	}

}

func (i *groupIterator) aggregateIteratorErrors() error {
	var messages []string
	for _, child := range i.Iterators {
		if child.Status() == QueryStatusError {
			messages = append(messages, fmt.Sprintf("connection '%s': %s", child.ConnectionName(), child.Error().Error()))
		}
	}
	if len(messages) > 0 {
		return fmt.Errorf("%d %s failed: \n%s",
			len(messages),
			utils.Pluralize("connections", len(messages)),
			strings.Join(messages, "\n"))
	}
	return nil
}

func (i *groupIterator) Close(writeToCache bool) {
	// TODO this may need more work
	for _, it := range i.Iterators {
		if it.Status() == QueryStatusStarted {
			it.Close(writeToCache)
		}
	}
	i.iteratorsRunning = 0
}

func (i *groupIterator) CanIterate() bool {
	switch i.Status() {
	case QueryStatusError, QueryStatusComplete:
		return false
	default:
		return true
	}
}
