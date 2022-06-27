package hub

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"github.com/turbot/steampipe/pkg/utils"
	"go.opentelemetry.io/otel/attribute"
)

// groupIterator is a struct which aggregates the results of as number of interators
type groupIterator struct {
	Name              string
	Iterators         []Iterator
	rowChan           chan map[string]interface{}
	childrenRunningWg sync.WaitGroup
	traceCtx          *telemetry.TraceCtx
}

func NewGroupIterator(name string, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, connectionConfig *modconfig.Connection, h *Hub, scanTraceCtx *telemetry.TraceCtx) (Iterator, error) {
	res := &groupIterator{
		Name: name,
		// create a buffered channel
		rowChan:  make(chan map[string]interface{}, rowBufferSize),
		traceCtx: scanTraceCtx,
	}

	// add aggregator specific attributes to span
	scanTraceCtx.Span.SetAttributes(
		attribute.String("connection_type", "aggregator"),
		attribute.StringSlice("connections", connectionConfig.ConnectionNames),
	)

	var errors []error
	for connectionName := range connectionConfig.Connections {
		// create a child span for this connection
		ctx, span := telemetry.StartSpan(scanTraceCtx.Ctx, constants.FdwName, "ChildConnection.Scan (%s)", table)
		span.SetAttributes(
			attribute.String("connection", connectionName),
		)
		connectionTraceCtx := &telemetry.TraceCtx{Ctx: ctx, Span: span}

		iterator, err := h.startScanForConnection(connectionName, table, qualMap, columns, limit, connectionTraceCtx)
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
		log.Printf("[TRACE] groupIterator len(row) == 0 ")

		// TODO check not already closed? NEED A LOCK??
		// close the span
		i.traceCtx.Span.End()

		return nil, i.aggregateIteratorErrors()
	}
	return row, nil

}

func (i *groupIterator) start() {
	// start a goroutine for each iterator
	for _, child := range i.Iterators {
		i.childrenRunningWg.Add(1)
		log.Printf("[TRACE] groupIterator (%p) start() connection: %s it: %p ", i, child.ConnectionName(), child)
		// increment th running count
		go i.streamIteratorResults(child)
	}
	// start a goroutine which waits for all children to complete
	go i.waitForChildren()
}

func (i *groupIterator) waitForChildren() {
	// when ALL children are complete, stream a nil row
	i.childrenRunningWg.Wait()
	log.Printf("[TRACE] groupIterator (%p) waitForChildren() - children complete, sending nil  row", i)
	i.rowChan <- nil
}

func (i *groupIterator) streamIteratorResults(child Iterator) {
	log.Printf("[TRACE] streamIteratorResults connection %s", child.ConnectionName())
	for {
		// call next - ignore error as the iterator state will store it
		row, _ := child.Next()
		// if no row was returned, we are done
		if len(row) == 0 {
			log.Printf("[TRACE] groupIterator (%p) streamIteratorResults connection %s empty row", i, child.ConnectionName())
			i.childrenRunningWg.Done()
			return
		}
		// stream the row
		i.rowChan <- row
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
	log.Printf("[TRACE] groupIterator.Close writeToCache=%v", writeToCache)

	for _, it := range i.Iterators {
		if it.Status() == QueryStatusStarted {
			it.Close(writeToCache)
		} else {
			log.Printf("[TRACE] groupIterator.Close iterator %s not running (%s), so not closing", it.ConnectionName(), it.Status())
		}
	}
	// TODO check not already closed?
	// close the span
	i.traceCtx.Span.End()
}

func (i *groupIterator) CanIterate() bool {
	switch i.Status() {
	case QueryStatusError, QueryStatusComplete:
		return false
	default:
		return true
	}
}

func (i *groupIterator) GetScanMetadata() []ScanMetadata {
	var res []ScanMetadata
	for _, iter := range i.Iterators {
		res = append(res, iter.GetScanMetadata()...)
	}
	return res
}

func (i *groupIterator) GetTraceContext() *telemetry.TraceCtx {
	return i.traceCtx
}
