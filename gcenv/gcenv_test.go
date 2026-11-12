package gcenv

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
)

// The parse tests use the Go runtime as the reference. For each value they start this test
// binary again with the variable set. The child is a normal Go program, so its runtime reads
// the variable at start. The child prints what the runtime read, and the test compares that
// with what parseGOGC or parseGOMEMLIMIT returns.

const helperEnv = "GCENV_TEST_HELPER"

// TestHelperProcess is not a real test. It runs only in a child that runtimeSetting starts.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("runs only as a child of runtimeSetting")
	}
	fmt.Fprintf(os.Stdout, "%d %d\n", debug.SetGCPercent(100), debug.SetMemoryLimit(-1))
	os.Exit(0)
}

// runtimeSetting starts a child with key=value in its environment. It returns the GC percent
// and the memory limit that the child's runtime read at start. ok is false if the runtime
// refused the value and stopped the child.
func runtimeSetting(t *testing.T, key, value string) (percent int, limit int64, ok bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "GOGC=") && !strings.HasPrefix(e, "GOMEMLIMIT=") {
			env = append(env, e)
		}
	}
	cmd.Env = append(env, helperEnv+"=1", key+"="+value)
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(string(out), "%d %d", &percent, &limit); err != nil {
		t.Fatalf("%s=%q: cannot read child output %q: %v", key, value, out, err)
	}
	return percent, limit, true
}

func TestParseGOGCMatchesRuntime(t *testing.T) {
	for _, v := range []string{
		"off", "100", "50", "0", "-1", "+20", "007",
		"OFF", "abc", "1e3", "50%", " 50", "2147483647", "2147483648", "-2147483649",
	} {
		t.Run(v, func(t *testing.T) {
			want, _, ok := runtimeSetting(t, "GOGC", v)
			if !ok {
				t.Fatalf("GOGC=%q: the child did not start", v)
			}
			if got := parseGOGC(v); got != want {
				t.Errorf("parseGOGC(%q) = %d, the Go runtime read %d", v, got, want)
			}
		})
	}
}

func TestParseGOMEMLIMITMatchesRuntime(t *testing.T) {
	for _, v := range []string{
		// valid
		"off", "0", "1024", "+64", "100B", "1KiB", "8MiB", "2GiB", "1TiB",
		"9223372036854775807", "9223372036854775807B", "8388607TiB",
		// not valid
		"OFF", "-1", "8MB", "8KB", "8mib", "8 MiB", "1.5GiB", "MiB", "B", "iB", "5iB", "8XiB",
		"8MiBB", "0x10", "8388608TiB", "9223372036854775808",
	} {
		t.Run(v, func(t *testing.T) {
			_, want, wantOK := runtimeSetting(t, "GOMEMLIMIT", v)
			got, ok := parseGOMEMLIMIT(v)
			if ok != wantOK || (ok && got != want) {
				t.Errorf("parseGOMEMLIMIT(%q) = %d, %v; the Go runtime read %d, %v", v, got, ok, want, wantOK)
			}
		})
	}
}

// keepGCSettings restores the GC percent and memory limit of this process after the test.
func keepGCSettings(t *testing.T) {
	percent := debug.SetGCPercent(100)
	debug.SetGCPercent(percent)
	limit := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(percent)
		debug.SetMemoryLimit(limit)
	})
}

func currentGCPercent() int {
	p := debug.SetGCPercent(100)
	debug.SetGCPercent(p)
	return p
}

// The Apply tests set the variables after this process started. That is the FDW's case: the
// runtime has already started, so it ignores the new values until Apply runs.
func TestApply(t *testing.T) {
	tests := []struct {
		name        string
		gogc        string
		gomemlimit  string
		wantPercent int
		wantLimit   int64
	}{
		{name: "not set", wantPercent: 100, wantLimit: math.MaxInt64},
		{name: "GOGC off", gogc: "off", wantPercent: -1, wantLimit: math.MaxInt64},
		{name: "GOGC 50", gogc: "50", wantPercent: 50, wantLimit: math.MaxInt64},
		{name: "GOMEMLIMIT 64MiB", gomemlimit: "64MiB", wantPercent: 100, wantLimit: 64 << 20},
		{name: "both", gogc: "200", gomemlimit: "1GiB", wantPercent: 200, wantLimit: 1 << 30},
		{name: "GOMEMLIMIT not valid", gogc: "75", gomemlimit: "64MB", wantPercent: 75, wantLimit: math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keepGCSettings(t)
			debug.SetGCPercent(100)
			debug.SetMemoryLimit(math.MaxInt64)
			t.Setenv("GOGC", tt.gogc)
			t.Setenv("GOMEMLIMIT", tt.gomemlimit)

			if p := currentGCPercent(); p != 100 {
				t.Fatalf("before Apply: GC percent %d, want 100: the runtime must not read GOGC after start", p)
			}

			Apply()

			if p := currentGCPercent(); p != tt.wantPercent {
				t.Errorf("GC percent = %d, want %d", p, tt.wantPercent)
			}
			if l := debug.SetMemoryLimit(-1); l != tt.wantLimit {
				t.Errorf("memory limit = %d, want %d", l, tt.wantLimit)
			}
		})
	}
}
