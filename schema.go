package main

/*
#cgo CFLAGS: -I../fdw -Ifdw/include/postgresql/server -Ifdw/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

import (
	"log"
	"unsafe"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-postgres-fdw/sql"
)

func SchemaToSql(schema map[string]*proto.TableSchema, stmt *C.ImportForeignSchemaStmt, serverOid C.Oid) *C.List {
	var commands *C.List

	server := C.GetForeignServer(serverOid)
	if server == nil {
		return nil
	}
	serverName := C.GoString(server.servername)
	localSchema := C.GoString(stmt.local_schema)

	// first figure out which tables we want
	tables := []string{}
	// iterate over table list
	if stmt.table_list != nil {
		for it := stmt.table_list.head; it != nil; it = it.next {
			var rv *C.RangeVar = C.cellGetRangeVar(it)
			t := C.GoString(rv.relname)
			tables = append(tables, t)
		}
	}
	log.Printf("[TRACE] tables %v\n", tables)

	// TODO we do not handle any options currently

	for table, tableSchema := range schema {
		if stmt.list_type == C.FDW_IMPORT_SCHEMA_LIMIT_TO {
			log.Printf("[TRACE] list_type is FDW_IMPORT_SCHEMA_LIMIT_TO: %v", tables)

			if !helpers.StringSliceContains(tables, table) {
				log.Printf("[TRACE] Skipping table %s", table)

				continue
			}
		} else if stmt.list_type == C.FDW_IMPORT_SCHEMA_EXCEPT {
			log.Printf("[TRACE] list_type is FDW_IMPORT_SCHEMA_EXCEPT: %v", tables)

			if helpers.StringSliceContains(tables, table) {
				log.Printf("[TRACE] Skipping table %s", table)
				continue
			}
		}
		log.Printf("[TRACE] Import table  %s", table)

		sql, err := sql.GetSQLForTable(table, tableSchema, localSchema, serverName)
		if err != nil {
			FdwError(err)
			return nil
		}
		log.Printf("[INFO] Table sql: \n%s\n", sql)
		commands = C.lappend(commands, unsafe.Pointer(C.CString(sql)))
	}

	return commands
}
