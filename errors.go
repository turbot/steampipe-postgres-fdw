package main

/*
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"
import "unsafe"

func FdwError(e error) {
	cmsg := C.CString(e.Error())
	defer C.free(unsafe.Pointer(cmsg))
	C.fdw_errorReport(C.ERROR, C.ERRCODE_FDW_ERROR, cmsg)
}

func FdwErrorReport(level int, code int, msg string, hint string) {
	cmsg := C.CString(msg)
	defer C.free(unsafe.Pointer(cmsg))
	chint := C.CString(hint)
	defer C.free(unsafe.Pointer(chint))
	C.fdw_errorReportWithHint(C.ERROR, C.ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE, cmsg, chint)
}
