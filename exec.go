package main

/*
#cgo CFLAGS:  -I../fdw -I../fdw/include/postgresql/server -I../fdw/include/postgresql/internal
#include "postgres.h"
#include "common.h"

typedef struct GoFdwExecutionState
{
 uint tok;
} GoFdwExecutionState;

static inline GoFdwExecutionState* makeState(){
 GoFdwExecutionState *s = (GoFdwExecutionState *) malloc(sizeof(GoFdwExecutionState));
 return s;
}

static inline void freeState(GoFdwExecutionState * s){ if (s) free(s); }
*/
import "C"

import (
	"log"
	"sync"
	"unsafe"

	"github.com/turbot/steampipe-postgres-fdw/hub"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

type ExecState struct {
	Rel   *types.Relation
	Opts  map[string]string
	Iter  hub.Iterator
	State *C.FdwExecState
}

var (
	mu   sync.RWMutex
	si   uint64
	sess = make(map[uint64]*ExecState)
)

func SaveExecState(s *ExecState) unsafe.Pointer {
	mu.Lock()
	log.Println("[WARN] SaveExecState")
	si++
	i := si
	sess[i] = s
	mu.Unlock()
	cs := C.makeState()
	cs.tok = C.uint(i)
	return unsafe.Pointer(cs)
}

func ClearExecState(p unsafe.Pointer) {
	log.Println("[WARN] ClearExecState")
	if p == nil {
		return
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	mu.Lock()
	delete(sess, i)
	mu.Unlock()
	C.freeState(cs)
}

func GetExecState(p unsafe.Pointer) *ExecState {
	log.Println("[WARN] GetExecState")
	if p == nil {
		return nil
	}
	cs := (*C.GoFdwExecutionState)(p)
	i := uint64(cs.tok)
	mu.RLock()
	s := sess[i]
	mu.RUnlock()
	return s
}
