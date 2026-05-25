package hub

import "time"

// queryCancelPollInterval is how often the per-scan watcher goroutine asks
// Postgres whether the current backend has a pending cancellation. 250ms gives
// sub-second responsiveness to statement_timeout / pg_cancel_backend without
// noticeable cgo overhead (a single inline read of two sig_atomic_t globals).
const queryCancelPollInterval = 250 * time.Millisecond

// queryCancelChecker is set by the FDW (cgo) layer at init via
// SetQueryCancelChecker. It returns true when Postgres has set
// QueryCancelPending or ProcDiePending on the current backend. The hub uses
// this from a polling goroutine to bridge Postgres cancellation into the
// iterator's Go context, which is otherwise unreachable while the cgo
// IterateForeignScan call is blocked on a hung plugin gRPC stream (issue
// #671).
var queryCancelChecker func() bool

// SetQueryCancelChecker installs the function the hub uses to detect a
// pending Postgres query-cancel or backend-die request. It is intended to be
// called exactly once, from the FDW package's init(), with a cgo-backed
// implementation.
func SetQueryCancelChecker(fn func() bool) {
	queryCancelChecker = fn
}

// isQueryCancelPending reports whether Postgres has requested cancellation of
// the current backend. Returns false if no checker has been installed (e.g.
// in unit tests that exercise the hub in isolation).
func isQueryCancelPending() bool {
	if queryCancelChecker == nil {
		return false
	}
	return queryCancelChecker()
}

// queryCancelCheckerConfigured reports whether a cancellation checker has
// been registered. Callers can use this to skip starting the per-scan
// watcher goroutine entirely when the bridge isn't wired up.
func queryCancelCheckerConfigured() bool {
	return queryCancelChecker != nil
}
