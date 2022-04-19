package main

/*
#cgo darwin CFLAGS: -I/usr/local/opt/postgresql@13/include/postgresql/server -I/usr/local/opt/postgresql@13/include/postgresql/internal
#cgo linux CFLAGS: -I/usr/include/postgresql/13/server -I/usr/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
#include "utils/rel.h"
#include "nodes/pg_list.h"
*/
import "C"

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-postgres-fdw/hub"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/constants"
)

var logger hclog.Logger

// force loading of this module
//export goInit
func goInit() {}

func init() {
	if logger != nil {
		return
	}
	log.Printf("[INFO] \n******************************************************\n\n\t\tsteampipe postgres fdw init\n\n******************************************************\n")

	// HACK: env vars do not all get copied into the Go env vars so explicitly copy them
	SetEnvVars()

	level := logging.LogLevel()
	log.Printf("[INFO] Log level %s\n", level)
	if level != "TRACE" {
		// suppress logs
		log.SetOutput(ioutil.Discard)
	}
	logger = logging.NewLogger(&hclog.LoggerOptions{
		Name:       "hub",
		TimeFn:     func() time.Time { return time.Now().UTC() },
		TimeFormat: "2006-01-02 15:04:05.000 UTC",
	})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
}

//export goFdwGetRelSize
func goFdwGetRelSize(state *C.FdwPlanState, root *C.PlannerInfo, rows *C.double, width *C.int, baserel *C.RelOptInfo) {
	logging.ClearProfileData()

	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
		return
	}

	// reload connection config
	_, err = pluginHub.LoadConnectionConfig()
	if err != nil {
		log.Printf("[ERROR] LoadConnectionConfig failed %v ", err)
		FdwError(err)
		return
	}

	opts := GetFTableOptions(types.Oid(state.foreigntableid))

	// build columns
	var columns []string
	if state.target_list != nil {
		columns = CStringListToGoArray(state.target_list)
	}

	result, err := pluginHub.GetRelSize(columns, nil, opts)
	if err != nil {
		log.Println("[ERROR] pluginHub.GetRelSize")
		FdwError(err)
		return
	}

	*rows = C.double(result.Rows)
	*width = C.int(result.Width)

	return
}

//export goFdwGetPathKeys
func goFdwGetPathKeys(state *C.FdwPlanState) *C.List {
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}
	var result *C.List
	opts := GetFTableOptions(types.Oid(state.foreigntableid))
	ftable := C.GetForeignTable(state.foreigntableid)
	rel := C.RelationIdGetRelation(ftable.relid)
	defer C.RelationClose(rel)
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = getNamespace(rel)

	if opts["connection"] == constants.CommandSchema {
		FdwError(fmt.Errorf("cannot select from command schema"))
		return nil
	}

	// ask the hub for path keys - it will use the table schema to create path keys for all key columns
	pathKeys, err := pluginHub.GetPathKeys(opts)
	if err != nil {
		FdwError(err)
	}

	for _, pathKey := range pathKeys {
		var item *C.List
		var attnums *C.List
		for _, key := range pathKey.ColumnNames {
			// Lookup the attribute number by its key.
			for k := 0; k < int(state.numattrs); k++ {
				ci := C.getConversionInfo(state.cinfos, C.int(k))
				if ci == nil {
					continue
				}
				if key == C.GoString(ci.attrname) {
					attnums = C.list_append_unique_int(attnums, ci.attnum)
					break
				}
			}
		}

		item = C.lappend(item, unsafe.Pointer(attnums))
		item = C.lappend(item, unsafe.Pointer(C.makeConst(C.INT4OID, -1, C.InvalidOid, 4, C.ulong(pathKey.Rows), false, true)))
		result = C.lappend(result, unsafe.Pointer(item))
	}

	return result
}

//export goFdwExplainForeignScan
func goFdwExplainForeignScan(node *C.ForeignScanState, es *C.ExplainState) {
	s := GetExecState(node.fdw_state)
	if s == nil {
		return
	}
	// Produce extra output for EXPLAIN
	if e, ok := s.Iter.(Explainable); ok {
		e.Explain(Explainer{ES: es})
	}
	ClearExecState(node.fdw_state)
	node.fdw_state = nil
}

//export goFdwBeginForeignScan
func goFdwBeginForeignScan(node *C.ForeignScanState, eflags C.int) {
	logging.LogTime("[fdw] BeginForeignScan start")
	rel := BuildRelation(node.ss.ss_currentRelation)
	opts := GetFTableOptions(rel.ID)
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = rel.Namespace

	log.Printf("[INFO] goFdwBeginForeignScan, connection '%s', table '%s' \n", opts["connection"], opts["table"])

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwBeginForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	// retrieve exec state
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	var execState *C.FdwExecState = C.initializeExecState(unsafe.Pointer(plan.fdw_private))

	var columns []string
	if execState.target_list != nil {
		columns = CStringListToGoArray(execState.target_list)
	}

	// get conversion info
	var tupdesc C.TupleDesc = node.ss.ss_currentRelation.rd_att
	C.initConversioninfo(execState.cinfos, C.TupleDescGetAttInMetadata(tupdesc))

	// create a wrapper struct for cinfos
	cinfos := newConversionInfos(execState)
	quals := restrictionsToQuals(node, cinfos)

	// start the plugin hub
	var err error
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}

	iter, err := pluginHub.Scan(columns, quals, int64(execState.limit), opts)
	if err != nil {
		log.Printf("[WARN] pluginHub.Scan FAILED: %s", err)
		FdwError(err)
		return
	}

	s := &ExecState{
		Rel:   rel,
		Opts:  opts,
		Iter:  iter,
		State: execState,
	}

	log.Printf("[TRACE] goFdwBeginForeignScan: save exec state %v\n", s)
	node.fdw_state = SaveExecState(s)

	logging.LogTime("[fdw] BeginForeignScan end")
}

//export goFdwIterateForeignScan
func goFdwIterateForeignScan(node *C.ForeignScanState) *C.TupleTableSlot {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwIterateForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	logging.LogTime("[fdw] IterateForeignScan start")

	s := GetExecState(node.fdw_state)

	slot := node.ss.ss_ScanTupleSlot
	C.ExecClearTuple(slot)

	// call the iterator
	// row is a map of column name to value (as an interface)
	row, err := s.Iter.Next()
	if err != nil {
		FdwError(err)
		return slot
	}

	if len(row) == 0 {
		log.Printf("[TRACE] goFdwIterateForeignScan returned empty row - this scan complete (%p)", s.Iter)

		logging.LogTime("[fdw] IterateForeignScan end")
		// show profiling - ignore intervals less than 1ms
		//logging.DisplayProfileData(10*time.Millisecond, logger)
		return slot
	}

	isNull := make([]C.bool, len(s.Rel.Attr.Attrs))
	data := make([]C.Datum, len(s.Rel.Attr.Attrs))

	for i, attr := range s.Rel.Attr.Attrs {
		column := attr.Name

		var val = row[column]
		if val == nil {
			isNull[i] = C.bool(true)
			continue
		}
		// get the conversion info for this column
		ci := C.getConversionInfo(s.State.cinfos, C.int(i))
		// convert value into a datum
		if datum, err := ValToDatum(val, ci, s.State.buffer); err != nil {
			log.Printf("[WARN] goFdwIterateForeignScan ValToDatum error %v (%p)", err, s.Iter)
			FdwError(err)
			return slot
		} else {
			// everyone loves manually calculating array offsets
			data[i] = datum
		}
	}

	C.fdw_saveTuple(&data[0], &isNull[0], &node.ss)
	logging.LogTime("[fdw] IterateForeignScan end")

	return slot
}

//export goFdwReScanForeignScan
func goFdwReScanForeignScan(node *C.ForeignScanState) {
	log.Printf("[TRACE] goFdwReScanForeignScan")
	// restart the scan
	goFdwBeginForeignScan(node, 0)
}

//export goFdwEndForeignScan
func goFdwEndForeignScan(node *C.ForeignScanState) {
	s := GetExecState(node.fdw_state)
	pluginHub, _ := hub.GetHub()
	if s != nil && pluginHub != nil {
		log.Printf("[TRACE] goFdwEndForeignScan, iterator: %p", s.Iter)
		// is the iterator still running? If so it means postgres is stopping a scan before all rows have been read
		if s.Iter.Status() == hub.QueryStatusStarted {
			// if we have identified a limit from the query (i.e. it is an ungrouped, unordered query from a single table)
			// then we can cache the result, using the limit in teh
			// but if we have NOT extracted a limit, w e cannot cache the results as we are not certain they are complete
			writeToCache := s.State.limit != -1
			log.Printf("[TRACE] ending scan before iterator complete - limit: %v, writeToCache: %v, iterator: %p", s.State.limit, writeToCache, s.Iter)
			s.Iter.Close(writeToCache)

		}

		pluginHub.RemoveIterator(s.Iter)
	}
	ClearExecState(node.fdw_state)
	node.fdw_state = nil
}

//export goFdwAbortCallback
func goFdwAbortCallback() {
	log.Printf("[TRACE] goFdwAbortCallback")
	if pluginHub, err := hub.GetHub(); err == nil {
		pluginHub.Abort()
	}
}

//export goFdwImportForeignSchema
func goFdwImportForeignSchema(stmt *C.ImportForeignSchemaStmt, serverOid C.Oid) *C.List {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwImportForeignSchema failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[TRACE] goFdwImportForeignSchema remote '%s' local '%s'\n", C.GoString(stmt.remote_schema), C.GoString(stmt.local_schema))
	// get the plugin hub,
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
		return nil
	}

	remoteSchema := C.GoString(stmt.remote_schema)
	localSchema := C.GoString(stmt.local_schema)

	// special handling for the command schema
	if remoteSchema == constants.CommandSchema {
		commandSchema := pluginHub.GetCommandSchema()
		sql := SchemaToSql(commandSchema, stmt, serverOid)
		return sql
	}

	schema, err := pluginHub.GetSchema(remoteSchema, localSchema)
	if err != nil {
		FdwError(err)
		return nil
	}
	return SchemaToSql(schema.Schema, stmt, serverOid)
}

//export goFdwExecForeignInsert
func goFdwExecForeignInsert(estate *C.EState, rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, planSlot *C.TupleTableSlot) *C.TupleTableSlot {
	// get the connection from the relation namespace
	relid := rinfo.ri_RelationDesc.rd_id
	rel := C.RelationIdGetRelation(relid)
	defer C.RelationClose(rel)
	connection := getNamespace(rel)
	// if this is a command insert, handle it
	if connection == constants.CommandSchema {
		return handleCommandInsert(rinfo, slot, rel)
	}

	return nil
}

func handleCommandInsert(rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, rel C.Relation) *C.TupleTableSlot {
	relid := rinfo.ri_RelationDesc.rd_id
	opts := GetFTableOptions(types.Oid(relid))

	switch opts["table"] {
	case constants.CacheCommandTable:
		// we know there is just a single column - operation
		var isNull C.bool
		datum := C.slot_getattr(slot, 1, &isNull)
		operation := C.GoString(C.fdw_datumGetString(datum))
		hub, err := hub.GetHub()
		if err != nil {
			FdwError(err)
			return nil
		}
		if err := hub.HandleCacheCommand(operation); err != nil {
			FdwError(err)
			return nil
		}
	}

	return nil

	/*
		here is how to fetch each attribute value:
		tupleDesc := buildTupleDesc(rel.rd_att)
		attributes := tupleDesc.Attrs
		for i, a := range attributes {
			var isNull C.bool
			datum := C.slot_getattr(slot, C.int(i+1), &isNull)
		}*/
}

//export goFdwShutdown
func goFdwShutdown() {
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}
	pluginHub.Close()
}

//export goFdwValidate
func goFdwValidate(coid C.Oid, opts *C.List) {
	// Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
	// USER MAPPING or FOREIGN TABLE that uses fdw.
	// Raise an ERROR if the option or its value are considered invalid
	// or a required option is missing.
}

// required by buildmode=c-archive
func main() {}
