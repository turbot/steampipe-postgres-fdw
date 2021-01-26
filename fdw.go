package main

/*
#cgo CFLAGS: -Ifdw -Ifdw/include/postgresql/server -Ifdw/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

import (
	"fmt"
	"io/ioutil"
	"log"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-postgres-fdw/hub"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

var logger hclog.Logger

func init() {

	log.Printf("[INFO] \n******************************************************\n\n\t\tsteampipe postgres fdw init\n\n******************************************************\n")

	// HACK: env vars do not all get copied into the Go env vars so explicitly copy them
	SetEnvVars()

	level := logging.LogLevel()
	log.Printf("[INFO] Log level %s\n", level)
	if level != "TRACE" {
		// suppress logs
		log.SetOutput(ioutil.Discard)
	}

	logger = logging.NewLogger(&hclog.LoggerOptions{Name: "hub"})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)

}

//export getRelSize
func getRelSize(state *C.FdwPlanState, root *C.PlannerInfo, rows *C.double, width *C.int, baserel *C.RelOptInfo) {
	logging.ClearProfileData()

	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
		return
	}
	opts := GetFTableOptions(types.Oid(state.foreigntableid))

	var columns []string
	if state.target_list != nil {
		columns = CListToGoArray(state.target_list)
	}
	qualList := QualDefsToQuals(state.qual_list, state.cinfos)
	for _, q := range qualList {
		log.Printf("[TRACE] field '%s' operator '%s' value '%v'\n", q.FieldName, q.Operator, q.Value)
	}

	// Run the go interface
	result, err := pluginHub.GetRelSize(columns, qualList, opts)
	if err != nil {
		FdwError(err)
		return
	}

	*rows = C.double(result.Rows)
	*width = C.int(result.Width)

	return
}

//export getPathKeys
func getPathKeys(state *C.FdwPlanState) *C.List {
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}
	var result *C.List

	opts := GetFTableOptions(types.Oid(state.foreigntableid))

	// Run the go interface
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

//export fdwExplainForeignScan
func fdwExplainForeignScan(node *C.ForeignScanState, es *C.ExplainState) {
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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("goFdwBeginForeignScan failed with panic: %v", r)
		}
	}()

	log.Println("[TRACE] goFdwBeginForeignScan")

	// retrieve exec state
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	var execState *C.FdwExecState = C.initializeExecState(unsafe.Pointer(plan.fdw_private))

	var columns []string
	if execState.target_list != nil {
		columns = CListToGoArray(execState.target_list)
	}
	qualList := QualDefsToQuals(execState.qual_list, execState.cinfos)

	logging.LogTime("[gum] BeginForeignScan start")
	// start the plugin manager
	var err error
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}

	rel := BuildRelation(node.ss.ss_currentRelation)

	opts := GetFTableOptions(rel.ID)

	iter, err := pluginHub.Scan(rel, columns, qualList, opts)

	log.Printf("[TRACE] pluginHub.Scan returned %v\n", err)
	if err != nil {

		FdwError(err)
		return
	}

	s := &ExecState{
		Rel:   rel,
		Opts:  opts,
		Iter:  iter,
		State: execState,
	}
	log.Printf("[TRACE] save state %v\n", s)
	node.fdw_state = SaveExecState(s)

	logging.LogTime("[gum] BeginForeignScan end")
}

//export goFdwIterateForeignScan
func goFdwIterateForeignScan(node *C.ForeignScanState) *C.TupleTableSlot {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("goFdwIterateForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	logging.LogTime("[gum] IterateForeignScan start")

	s := GetExecState(node.fdw_state)

	slot := node.ss.ss_ScanTupleSlot
	C.ExecClearTuple(slot)

	log.Printf("[TRACE] NEXT \n")

	// row is a map of column name to value (as an interface)
	row, err := s.Iter.Next()

	if err != nil {
		FdwError(err)
		return slot
	}

	if len(row) == 0 {
		logging.LogTime("[gum] IterateForeignScan end")
		// show profiling - ignore intervals less than 1ms
		//logging.DisplayProfileData(10*time.Millisecond, logger)
		return slot
	}

	isNull := make([]C.bool, len(s.Rel.Attr.Attrs))
	data := make([]C.Datum, len(s.Rel.Attr.Attrs))

	for i, attr := range s.Rel.Attr.Attrs {
		column := attr.Name
		columnType := attr.Type
		var val = row[column]
		if val == nil {
			log.Printf("[TRACE] column NULL\n")
			isNull[i] = C.bool(true)
			continue
		}
		// get the conversion info for this column
		ci := C.getConversionInfo(s.State.cinfos, C.int(i))

		// convert value into a datum
		if datum, err := ValToDatum(val, ci, s.State.buffer); err != nil {
			FdwError(err)
			return slot
		} else {
			log.Printf("[TRACE] value: %v columnType %v, datum: %v\n", val, columnType, datum)
			// everyone loves manually calculating array offsets
			data[i] = datum
		}
	}
	C.fdw_saveTuple(&data[0], &isNull[0], &node.ss)

	logging.LogTime("[gum] IterateForeignScan end")
	return slot
}

//export fdwReScanForeignScan
func fdwReScanForeignScan(node *C.ForeignScanState) {
	// Rescan table, possibly with new parameters
	s := GetExecState(node.fdw_state)
	s.Iter.Reset(nil, nil, nil)
}

//export fdwEndForeignScan
func fdwEndForeignScan(node *C.ForeignScanState) {
	ClearExecState(node.fdw_state)
	node.fdw_state = nil
}

//export goFdwImportForeignSchema
func goFdwImportForeignSchema(stmt *C.ImportForeignSchemaStmt, serverOid C.Oid) *C.List {
	log.Printf("[WARN] goFdwImportForeignSchema remote '%s' local '%s'\n", C.GoString(stmt.remote_schema), C.GoString(stmt.local_schema))
	// get the plugin hub,
	// NOTE: refresh connection config
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
		return nil
	}
	// reload conecction config - the ImportSchema command may be called because the config has been changed
	connectionConfigChanged, err := pluginHub.LoadConnectionConfig()
	if err != nil {
		FdwError(err)
		return nil
	}

	remoteSchema := C.GoString(stmt.remote_schema)
	localSchema := C.GoString(stmt.local_schema)

	// if the connection config has changed locally, send it to the plugin
	if connectionConfigChanged {
		err := pluginHub.SetConnectionConfig(remoteSchema, localSchema)
		if err != nil {
			FdwError(err)
			return nil
		}

	}
	schema, err := pluginHub.GetSchema(remoteSchema, localSchema)
	if err != nil {
		FdwError(err)
		return nil
	}
	return SchemaToSql(schema.Schema, stmt, serverOid)
}

//export goFdwShutdown
func goFdwShutdown() {
	pluginHub, err := hub.GetHub()
	if err != nil {
		FdwError(err)
	}
	pluginHub.Close()

}

//export fdwValidate
func fdwValidate(coid C.Oid, opts *C.List) {
	// Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
	// USER MAPPING or FOREIGN TABLE that uses fdw.
	// Raise an ERROR if the option or its value are considered invalid
	// or a required option is missing.
}

// required by buildmode=c-archive
func main() {
	log.Println("[TRACE] MAIN")
}
