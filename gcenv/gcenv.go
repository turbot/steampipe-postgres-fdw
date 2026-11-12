// Package gcenv applies the GOGC and GOMEMLIMIT environment variables to the Go runtime.
//
// A Go program reads GOGC and GOMEMLIMIT when its runtime starts. In the FDW this does not
// work. The runtime starts when a Postgres backend loads the FDW library, and it reads the
// environment through the pointers after argv. These pointers go to the original environment
// strings. On Linux, Postgres has already copied the environment to new memory and written
// the process title over the original strings (see src/backend/utils/misc/ps_status.c).
// So the runtime sees no variables and starts with the default GC settings.
//
// Apply reads the two variables from the environment again, after the FDW has copied the
// Postgres environment into Go, and sets them with the runtime/debug functions.
package gcenv

import (
	"log"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// Apply sets the GC percent from GOGC and the memory limit from GOMEMLIMIT.
// It does not change a setting if its variable is empty or not set.
// If GOMEMLIMIT is not valid, Apply logs a warning and does not change the memory limit.
// (The Go runtime stops the process for a GOMEMLIMIT that is not valid. That would stop
// the Postgres backend, so Apply does not do that.)
func Apply() {
	if v := os.Getenv("GOGC"); v != "" {
		percent := parseGOGC(v)
		debug.SetGCPercent(percent)
		log.Printf("[INFO] GOGC=%s: GC percent set to %d", v, percent) //nolint:gosec // logs the process's own environment variable, not external input
	}
	if v := os.Getenv("GOMEMLIMIT"); v != "" {
		limit, ok := parseGOMEMLIMIT(v)
		if !ok {
			log.Printf("[WARN] GOMEMLIMIT=%s is not valid: memory limit not changed. See `go doc runtime/debug.SetMemoryLimit`", v) //nolint:gosec // logs the process's own environment variable, not external input
			return
		}
		debug.SetMemoryLimit(limit)
		log.Printf("[INFO] GOMEMLIMIT=%s: memory limit set to %d bytes", v, limit) //nolint:gosec // logs the process's own environment variable, not external input
	}
}

// parseGOGC parses GOGC as the Go runtime does: "off" is -1 (GC off), an integer is the GC
// percent, and any other value is the default, 100.
func parseGOGC(s string) int {
	if s == "off" {
		return -1
	}
	if n, err := strconv.ParseInt(s, 10, 32); err == nil {
		return int(n)
	}
	return 100
}

// parseGOMEMLIMIT parses GOMEMLIMIT as the Go runtime does: "off" is no limit, and other
// values are a byte count with an optional B, KiB, MiB, GiB or TiB suffix.
func parseGOMEMLIMIT(s string) (int64, bool) {
	if s == "off" {
		return math.MaxInt64, true
	}
	shift := 0
	for i, unit := range []string{"KiB", "MiB", "GiB", "TiB"} {
		if strings.HasSuffix(s, unit) {
			s, shift = strings.TrimSuffix(s, unit), 10*(i+1)
			break
		}
	}
	if shift == 0 {
		s = strings.TrimSuffix(s, "B")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > math.MaxInt64>>shift {
		return 0, false
	}
	return n << shift, true
}
