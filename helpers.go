package main

/*
#cgo CFLAGS: -Ifdw -Ifdw/include/postgresql/server -Ifdw/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"unsafe"

	"github.com/golang/protobuf/ptypes/timestamp"
	typeHelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

// convert a C list into a go array
func CListToGoArray(values *C.List) []string {
	ensureUnique := map[string]bool{}
	targets := []string{}
	for it := values.head; it != nil; it = it.next {
		val := C.cellGetValue(it)
		s := C.GoString(C.valueString(val))
		if !ensureUnique[s] {
			targets = append(targets, s)
			ensureUnique[s] = true
		}
	}
	return targets
}

// HACK: env vars do not all get copied into the Go env vars so explicitly copy them
func SetEnvVars() {
	var penv **C.char = C.environ
	s := C.GoString(*C.environ)

	for s != "" {
		idx := strings.Index(s, "=")
		key := s[:idx]
		value := s[idx+1:]
		os.Setenv(key, value)
		penv = C.incStringPointer(penv)
		s = C.GoString(*penv)
	}
}

func GetFTableOptions(id types.Oid) types.Options {
	// TODO - We need a sanitized form of the table name, e.g. all lowercase
	f := C.GetForeignTable(C.Oid(id))

	tmp := getOptions(f.options)
	return tmp
}

func getOptions(opts *C.List) types.Options {
	m := make(types.Options)
	for it := opts.head; it != nil; it = it.next {
		el := C.cellGetDef(it)
		name := C.GoString(el.defname)
		val := C.GoString(C.defGetString(el))
		m[name] = val
	}
	return m
}

func BuildRelation(rel C.Relation) *types.Relation {
	r := &types.Relation{
		ID:        types.Oid(rel.rd_id),
		IsValid:   fdwBool(rel.rd_isvalid),
		Attr:      buildTupleDesc(rel.rd_att),
		Namespace: getNamespace(rel),
	}
	return r
}

func getNamespace(rel C.Relation) string {
	schema := C.get_namespace_name(C.fdw_relationGetNamespace(rel))
	return C.GoString(schema)
}

func fdwBool(b C.bool) bool {
	return bool(b)
}

func fdwString(p unsafe.Pointer, n int) string {
	b := C.GoBytes(p, C.int(n))
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		i = len(b)
	}
	return string(b[:i])
}

func buildTupleDesc(desc C.TupleDesc) *types.TupleDesc {
	if desc == nil {
		return nil
	}
	d := &types.TupleDesc{
		TypeID:  types.Oid(desc.tdtypeid),
		TypeMod: int(desc.tdtypmod),
		//HasOid:  fdwBool(desc.tdhasoid),
		Attrs: make([]types.Attr, 0, int(desc.natts)),
	}
	for i := 0; i < cap(d.Attrs); i++ {
		p := C.fdw_tupleDescAttr(desc, C.int(i))
		d.Attrs = append(d.Attrs, buildAttr(p))
	}
	return d
}

const nameLen = C.NAMEDATALEN

func buildAttr(attr *C.FormData_pg_attribute) (out types.Attr) {
	out.Name = fdwString(unsafe.Pointer(&attr.attname.data[0]), nameLen)
	out.Type = types.Oid(attr.atttypid)
	out.Dimensions = int(attr.attndims)
	out.NotNull = fdwBool(attr.attnotnull)
	out.Dropped = fdwBool(attr.attisdropped)
	return
}

// convert a value from C StringInfo buffer into a C Datum
func ValToDatum(val interface{}, cinfo *C.ConversionInfo, buffer C.StringInfo) (C.Datum, error) {
	// init an empty return result
	datum := C.fdw_cStringGetDatum(C.CString(""))

	// write value into C buffer
	if err := valToBuffer(val, cinfo.atttypoid, buffer); err != nil {
		return datum, err
	}

	if buffer.len >= 0 {
		if cinfo.atttypoid == C.BYTEAOID ||
			cinfo.atttypoid == C.TEXTOID ||
			cinfo.atttypoid == C.VARCHAROID {
			// Special case, since the value is already a byte string.
			datum = C.fdw_pointerGetDatum(unsafe.Pointer(C.cstring_to_text_with_len(buffer.data, buffer.len)))
		} else {
			datum = C.InputFunctionCall(cinfo.attinfunc,
				buffer.data,
				cinfo.attioparam,
				cinfo.atttypmod)
		}
	}
	return datum, nil
}

// write the value into the C StringInfo buffer
func valToBuffer(val interface{}, oid C.Oid, buffer C.StringInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	var valueString string
	// handle json explicitly
	if oid == C.JSONBOID {
		bytes, err := json.Marshal(val)
		if err != nil {
			return err
		}
		valueString = string(bytes)
	} else {
		valueString = typeHelpers.ToString(val)
	}

	C.resetStringInfo(buffer)
	C.appendBinaryStringInfo(buffer, C.CString(valueString), C.int(len(valueString)))
	return
}

func TimeToPgTime(t time.Time) int64 {
	// Postgres stores dates as microseconds since Jan 1, 2000
	// https://www.postgresql.org/docs/9.1/datatype-datetime.html
	ts := t.UTC()
	epoch := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	its := ts.Sub(epoch) / 1000
	return int64(its)
}

func PgTimeToTimestamp(t int64) *timestamp.Timestamp {
	log.Printf("[WARN] PgTimeToTimestamp %d", t)
	// Postgres stores dates as microseconds since Jan 1, 2000
	// https://www.postgresql.org/docs/9.1/datatype-datetime.html
	return &timestamp.Timestamp{Seconds: t * 1000, Nanos: 0}
}
