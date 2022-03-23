package main

/*
#cgo CFLAGS:  -I/usr/local/include/postgresql/server -I/usr/local/include/postgresql/internal -g
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"
*/
import "C"

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
	C.ExplainPropertyText(C.CString(k), C.CString(v), e.ES)
}
