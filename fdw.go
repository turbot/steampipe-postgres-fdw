package main

/*
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"

#include "utils/rel.h"
#include "nodes/pg_list.h"
#include "utils/timestamp.h"

static Name deserializeDeparsedSortListCell(ListCell *lc);

*/
import "C"

import (
	"fmt"
	"io"
	"log"
	"time"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/sperr"
	"github.com/turbot/steampipe-postgres-fdw/hub"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe-postgres-fdw/version"
	"github.com/turbot/steampipe/pkg/constants"
)

var logger hclog.Logger

// force loading of this module
//
//export goInit
func goInit() {}

func init() {
	if logger != nil {
		return
	}

	// HACK: env vars do not all get copied into the Go env vars so explicitly copy them
	SetEnvVars()

	level := logging.LogLevel()
	log.Printf("[INFO] Log level %s\n", level)
	if level != "TRACE" {
		// suppress logs
		log.SetOutput(io.Discard)
	}
	logger = logging.NewLogger(&hclog.LoggerOptions{
		Name:       "hub",
		TimeFn:     func() time.Time { return time.Now().UTC() },
		TimeFormat: "2006-01-02 15:04:05.000 UTC",
	})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
	// create hub
	if err := hub.CreateHub(); err != nil {
		panic(err)
	}
	log.Printf("[INFO] .\n******************************************************\n\n\t\tsteampipe postgres fdw init\n\n******************************************************\n")
	log.Printf("[INFO] Version:   v%s\n", version.FdwVersion.String())
	log.Printf("[INFO] Log level: %s\n", level)

}

//export goLog
func goLog(msg *C.char) {
	log.Println("[INFO] " + C.GoString(msg))
}

// Given a list of FdwDeparsedSortGroup and a FdwPlanState,
// construct a list FdwDeparsedSortGroup that can be pushed down
//
//export goFdwCanSort
func goFdwCanSort(deparsed *C.List, planstate *C.FdwPlanState) *C.List {
	// This will be the list of FdwDeparsedSortGroup items that can be pushed down
	var pushDownList *C.List = nil

	// Iterate over the deparsed list
	if deparsed == nil {
		return pushDownList
	}

	// Convert the sortable fields into a lookup
	sortableFields := getSortableFields(planstate.foreigntableid)
	if len(sortableFields) == 0 {
		return pushDownList
	}

	for it := C.list_head(deparsed); it != nil; it = C.lnext(deparsed, it) {
		deparsedSortGroup := C.cellGetFdwDeparsedSortGroup(it)
		columnName := C.GoString(C.nameStr(deparsedSortGroup.attname))

		supportedOrder := sortableFields[columnName]
		requiredOrder := proto.SortOrder_Asc
		if deparsedSortGroup.reversed {
			requiredOrder = proto.SortOrder_Desc
		}
		log.Println("[INFO] goFdwCanSort column", columnName, "supportedOrder", supportedOrder, "requiredOrder", requiredOrder)

		if supportedOrder == requiredOrder || supportedOrder == proto.SortOrder_All {
			log.Printf("[INFO] goFdwCanSort column %s can be pushed down", columnName)
			// add deparsedSortGroup to pushDownList
			pushDownList = C.lappend(pushDownList, unsafe.Pointer(deparsedSortGroup))
		} else {
			log.Printf("[INFO] goFdwCanSort column %s CANNOT be pushed down - not pushing down any further columns", columnName)
			break
		}
	}

	return pushDownList
}

func getSortableFields(foreigntableid C.Oid) map[string]proto.SortOrder {
	opts := GetFTableOptions(types.Oid(foreigntableid))
	connection := GetSchemaNameFromForeignTableId(types.Oid(foreigntableid))
	if connection == constants.InternalSchema || connection == constants.LegacyCommandSchema {
		return nil
	}

	tableName := opts["table"]
	pluginHub := hub.GetHub()
	return pluginHub.GetSortableFields(tableName, connection)
}

//export goFdwGetRelSize
func goFdwGetRelSize(state *C.FdwPlanState, root *C.PlannerInfo, rows *C.double, width *C.int, baserel *C.RelOptInfo) {
	logging.ClearProfileData()

	log.Printf("[TRACE] goFdwGetRelSize")

	pluginHub := hub.GetHub()

	// get connection name
	connName := GetSchemaNameFromForeignTableId(types.Oid(state.foreigntableid))

	log.Println("[TRACE] connection name:", connName)

	serverOpts := GetForeignServerOptionsFromFTableId(types.Oid(state.foreigntableid))
	err := pluginHub.ProcessImportForeignSchemaOptions(serverOpts, connName)
	if err != nil {
		FdwError(sperr.WrapWithMessage(err, "failed to process options"))
	}

	// reload connection config
	// TODO remove need for fdw to load connection config
	_, err = pluginHub.LoadConnectionConfig()
	if err != nil {
		log.Printf("[ERROR] LoadConnectionConfig failed %v ", err)
		FdwError(err)
		return
	}

	tableOpts := GetFTableOptions(types.Oid(state.foreigntableid))

	// build columns
	var columns []string
	if state.target_list != nil {
		columns = CStringListToGoArray(state.target_list)
	}

	result, err := pluginHub.GetRelSize(columns, nil, tableOpts)
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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwGetPathKeys failed with panic: %v", r)

			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[TRACE] goFdwGetPathKeys")
	pluginHub := hub.GetHub()

	var result *C.List
	opts := GetFTableOptions(types.Oid(state.foreigntableid))
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = GetSchemaNameFromForeignTableId(types.Oid(state.foreigntableid))

	if opts["connection"] == constants.InternalSchema || opts["connection"] == constants.LegacyCommandSchema {
		return result
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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwExplainForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[TRACE] goFdwExplainForeignScan")
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
			log.Printf("[WARN] goFdwExplainForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	// read the explain flag
	explain := eflags&C.EXEC_FLAG_EXPLAIN_ONLY == C.EXEC_FLAG_EXPLAIN_ONLY

	logging.LogTime("[fdw] BeginForeignScan start")
	rel := BuildRelation(node.ss.ss_currentRelation)
	opts := GetFTableOptions(rel.ID)
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = rel.Namespace

	log.Printf("[INFO] goFdwBeginForeignScan, connection '%s', table '%s', explain: %v \n", opts["connection"], opts["table"], explain)

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
	quals, unhandledRestrictions := restrictionsToQuals(node, cinfos)

	// start the plugin hub

	pluginHub := hub.GetHub()
	s := &ExecState{
		Rel:   rel,
		Opts:  opts,
		State: execState,
	}
	// if we are NOT explaining, create an iterator to scan for us
	if !explain {
		var sortOrder = getSortColumns(execState)
		log.Printf("[INFO] goFdwBeginForeignScan, table '%s', sortOrder: %v", opts["table"], sortOrder)

		ts := int64(C.GetSQLCurrentTimestamp(0))
		iter, err := pluginHub.GetIterator(columns, quals, unhandledRestrictions, int64(execState.limit), sortOrder, ts, opts)
		if err != nil {
			log.Printf("[WARN] pluginHub.GetIterator FAILED: %s", err)
			FdwError(err)
			return
		}
		s.Iter = iter
	}

	log.Printf("[TRACE] goFdwBeginForeignScan: save exec state %v\n", s)
	node.fdw_state = SaveExecState(s)

	logging.LogTime("[fdw] BeginForeignScan end")
}

func getSortColumns(state *C.FdwExecState) []*proto.SortColumn {
	sortGroups := state.pathkeys
	var res []*proto.SortColumn
	for it := C.list_head(sortGroups); it != nil; it = C.lnext(sortGroups, it) {
		deparsedSortGroup := C.cellGetFdwDeparsedSortGroup(it)
		columnName := C.GoString(C.nameStr(deparsedSortGroup.attname))
		requiredOrder := proto.SortOrder_Asc
		if deparsedSortGroup.reversed {
			requiredOrder = proto.SortOrder_Desc
		}

		res = append(res, &proto.SortColumn{
			Column: columnName,
			Order:  requiredOrder,
		})
	}
	return res
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
	pluginHub := hub.GetHub()

	log.Printf("[TRACE] goFdwIterateForeignScan, table '%s' (%p)", s.Opts["table"], s.Iter)
	// if the iterator has not started, start
	if s.Iter.Status() == hub.QueryStatusReady {
		log.Printf("[INFO] goFdwIterateForeignScan calling pluginHub.StartScan, table '%s' Current timestamp: %d (%p)", s.Opts["table"], s.Iter.GetQueryTimestamp(), s.Iter)
		if err := pluginHub.StartScan(s.Iter); err != nil {
			FdwError(err)
			return slot
		}
	}
	// call the iterator
	// row is a map of column name to value (as an interface)
	row, err := s.Iter.Next()
	if err != nil {
		log.Printf("[INFO] goFdwIterateForeignScan Next returned error: %s (%p)", err.Error(), s.Iter)
		FdwError(err)
		return slot
	}

	if len(row) == 0 {
		log.Printf("[INFO] goFdwIterateForeignScan returned empty row - this scan complete (%p)", s.Iter)
		// add scan metadata to hub
		pluginHub.AddScanMetadata(s.Iter)
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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwReScanForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	rel := BuildRelation(node.ss.ss_currentRelation)
	opts := GetFTableOptions(rel.ID)

	log.Printf("[INFO] goFdwReScanForeignScan, connection '%s', table '%s'", opts["connection"], opts["table"])
	// restart the scan
	goFdwBeginForeignScan(node, 0)
}

//export goFdwEndForeignScan
func goFdwEndForeignScan(node *C.ForeignScanState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwEndForeignScan failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	s := GetExecState(node.fdw_state)
	pluginHub := hub.GetHub()
	if s != nil {
		log.Printf("[INFO] goFdwEndForeignScan, iterator: %p", s.Iter)
		pluginHub.EndScan(s.Iter, int64(s.State.limit))
	}
	ClearExecState(node.fdw_state)
	node.fdw_state = nil

}

//export goFdwAbortCallback
func goFdwAbortCallback() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwAbortCallback failed with panic: %v", r)
			// DO NOT call FdwError or we will recurse
		}
	}()
	log.Printf("[INFO] goFdwAbortCallback")
	pluginHub := hub.GetHub()
	pluginHub.Abort()

}

//export goFdwImportForeignSchema
func goFdwImportForeignSchema(stmt *C.ImportForeignSchemaStmt, serverOid C.Oid) *C.List {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwImportForeignSchema failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[INFO] goFdwImportForeignSchema remote '%s' local '%s'\n", C.GoString(stmt.remote_schema), C.GoString(stmt.local_schema))
	// get the plugin hub,
	pluginHub := hub.GetHub()

	remoteSchema := C.GoString(stmt.remote_schema)
	localSchema := C.GoString(stmt.local_schema)

	// special handling for the command schema
	if remoteSchema == constants.InternalSchema {
		log.Printf("[INFO] importing setting tables into %s", remoteSchema)
		settingsSchema := pluginHub.GetSettingsSchema()
		sql := SchemaToSql(settingsSchema, stmt, serverOid)
		return sql
	}
	if remoteSchema == constants.LegacyCommandSchema {
		log.Printf("[INFO] importing setting tables into %s", remoteSchema)
		settingsSchema := pluginHub.GetLegacySettingsSchema()
		sql := SchemaToSql(settingsSchema, stmt, serverOid)
		return sql
	}

	fServer := C.GetForeignServer(serverOid)
	serverOptions := GetForeignServerOptions(fServer)

	log.Println("[TRACE] goFdwImportForeignSchema serverOptions:", serverOptions)

	err := pluginHub.ProcessImportForeignSchemaOptions(serverOptions, localSchema)
	if err != nil {
		FdwError(sperr.WrapWithMessage(err, "failed to process options"))
	}

	schema, err := pluginHub.GetSchema(remoteSchema, localSchema)
	if err != nil {
		log.Printf("[WARN] goFdwImportForeignSchema failed: %s", err)
		FdwError(err)
		return nil
	}
	res := SchemaToSql(schema.Schema, stmt, serverOid)

	return res
}

//export goFdwExecForeignInsert
func goFdwExecForeignInsert(estate *C.EState, rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, planSlot *C.TupleTableSlot) *C.TupleTableSlot {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwExecForeignInsert failed with panic: %v", r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	// get the connection from the relation namespace
	relid := rinfo.ri_RelationDesc.rd_id
	rel := C.RelationIdGetRelation(relid)
	defer C.RelationClose(rel)
	connection := getNamespace(rel)
	// if this is a command insert, handle it
	if connection == constants.InternalSchema || connection == constants.LegacyCommandSchema {
		return handleCommandInsert(rinfo, slot, rel)
	}

	return nil
}

func handleCommandInsert(rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, rel C.Relation) *C.TupleTableSlot {
	relid := rinfo.ri_RelationDesc.rd_id
	opts := GetFTableOptions(types.Oid(relid))
	pluginHub := hub.GetHub()

	switch opts["table"] {
	case constants.LegacyCommandTableCache:
		// we know there is just a single column - operation
		var isNull C.bool
		datum := C.slot_getattr(slot, 1, &isNull)
		operation := C.GoString(C.fdw_datumGetString(datum))
		if err := pluginHub.HandleLegacyCacheCommand(operation); err != nil {
			FdwError(err)
			return nil
		}

	case constants.ForeignTableSettings:
		tupleDesc := buildTupleDesc(rel.rd_att)
		attributes := tupleDesc.Attrs
		var key *string
		var value *string

		// iterate through the attributes
		for i, a := range attributes {
			var isNull C.bool
			datum := C.slot_getattr(slot, C.int(i+1), &isNull)
			if isNull {
				continue
			}
			// get a string from the memory slot
			datumStr := C.GoString(C.fdw_datumGetString(datum))

			log.Println("[TRACE] name", a.Name)
			log.Println("[TRACE] datum", datum)
			log.Println("[TRACE] datumstr", datumStr)

			// map it to one of key/value
			switch a.Name {
			case constants.ForeignTableSettingsKeyColumn:
				key = &datumStr
			case constants.ForeignTableSettingsValueColumn:
				value = &datumStr
			}
		}

		// if both key and value are not set, ERROR
		if key == nil || value == nil {
			FdwError(fmt.Errorf("invalid setting: both 'key' and 'value' columns need to be set"))
			return nil
		}

		// apply the setting
		if err := pluginHub.ApplySetting(*key, *value); err != nil {
			FdwError(err)
		}
		return nil

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
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] goFdwShutdown failed with panic: %v", r)
			// DO NOT call FdwError or we will recurse
		}
	}()
	log.Printf("[INFO] .\n******************************************************\n\n\t\tsteampipe postgres fdw shutdown\n\n******************************************************\n")
	pluginHub := hub.GetHub()
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
