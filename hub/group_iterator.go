package hub

import (
	"log"

	"github.com/turbot/go-kit/helpers"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
)

// groupIterator is a struct which aggregates the results of as number of interators
type groupIterator struct {
	Name      string
	Iterators []Iterator
	// current iterator index
	currentIterator int
}

func NewGroupIterator(name string, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, connections []string, h *Hub) (Iterator, error) {
	res := &groupIterator{Name: name}
	var errors []error
	for _, connectionName := range connections {
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
	return res, nil
}

func (i *groupIterator) ConnectionName() string {
	return i.Name
}

func (i *groupIterator) Status() queryStatus {
	// TODO
	return ""
}

func (i *groupIterator) Error() error {
	return nil
}

// Next implements Iterator
func (i *groupIterator) Next() (map[string]interface{}, error) {
	// are any iterators NOT in error state
	var row map[string]interface{}

	// find an iterator which can iterate - if none can return empty row
	for len(row) == 0 && i.canIterate() {
		row, _ = i.getRow()
	}

	// todo if this is an empty row, report error state of iterators
	return row, nil
}

func (i *groupIterator) getRow() (map[string]interface{}, error) {
	idx := i.currentIterator
	iterator := i.Iterators[idx]

	log.Printf("[INFO]  groupIterator getRow")
	// loop through iterators until we find one which can iterate
	for !iteratorCanIterate(iterator) {
		log.Printf("[INFO] iterator %s cannot iterate (status: %s)", iterator.ConnectionName(), iterator.Status())

		idx = i.incrementIteratorIndex(idx)
		// have we tried them all?
		if idx == i.currentIterator {
			log.Printf("[INFO: no iterators can iterate")
			return nil, nil
		}
		iterator = i.Iterators[idx]
	}

	log.Printf("[INFO] found iterator which can iterate: %s", iterator.ConnectionName())
	res, err := iterator.Next()

	// update current iterator ready for next time
	i.currentIterator = i.incrementIteratorIndex(idx)
	return res, err
}

func (i *groupIterator) incrementIteratorIndex(idx int) int {
	idx++
	if idx == len(i.Iterators) {
		idx = 0
	}
	return idx
}

// can any of the groupIterators children iterate
func (i *groupIterator) canIterate() bool {
	for _, it := range i.Iterators {
		if iteratorCanIterate(it) {
			return true
		}
	}
	return false

}

// can this iterator iterate?
func iteratorCanIterate(iterator Iterator) bool {
	status := iterator.Status()
	return status == queryStatusReady || status == queryStatusStarted
}

func (i *groupIterator) Close() {
	for _, it := range i.Iterators {
		it.Close()
	}
	i.currentIterator = 0
}
