package main

/*
#cgo darwin CFLAGS: -Ifdw -I/usr/local/include/postgresql/server -I/usr/local/include/postgresql/internal -g
#cgo linux CFLAGS: -Ifdw -I/usr/include/postgresql/14/server -I/usr/include/postgresql/14/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "postgres.h"
#include "common.h"
#include "fdw_helpers.h"
*/
import "C"

// safe wrapper for **C.ConversionInfo with array bounds checking
type conversionInfos struct {
	numAttrs int
	cinfos   **C.ConversionInfo
}

func newConversionInfos(execState *C.FdwExecState) *conversionInfos {
	return &conversionInfos{cinfos: execState.cinfos, numAttrs: int(execState.numattrs)}
}
func (c *conversionInfos) get(idx int) *C.ConversionInfo {
	if idx < c.numAttrs {
		return C.getConversionInfo(c.cinfos, C.int(idx))
	}
	return nil
}
