package main

/*
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"
import "unsafe"

// Explainable is an optional interface for Iterator that can explain it's execution plan.
type Explainable interface {
	// Explain is called during EXPLAIN query.
	Explain(e Explainer)
}

// Explainer is an helper build an EXPLAIN response.
type Explainer struct {
	ES *C.ExplainState
}

// Property adds a key-value property to results of EXPLAIN query.
func (e Explainer) Property(k, v string) {
	ck := C.CString(k)
	cv := C.CString(v)
	defer C.free(unsafe.Pointer(ck))
	defer C.free(unsafe.Pointer(cv))
	C.ExplainPropertyText(ck, cv, e.ES)
}
