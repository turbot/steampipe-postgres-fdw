package jsonb

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"no NUL", "plain", `"plain"`},
		{"NUL in a string", "a\x00b", `"ab"`},
		{"only a NUL", "\x00", `""`},
		{"runs of NULs", "\x00\x00a\x00", `"a"`},
		{"NUL, backslash, NUL", "x\x00\\\x00y", `"x\\y"`},
		{"NUL, two backslashes, NUL", "x\x00\\\\\x00y", `"x\\\\y"`},
		{"backslash, NUL", "\\\x00", `"\\"`},
		{"text \\u0000 is kept", `C:\u0000dir`, `"C:\\u0000dir"`},
		{"text \\\\u0000 is kept", `\\u0000`, `"\\\\u0000"`},
		{"text \\u0000 after a NUL is kept", "\x00\\u0000", `"\\u0000"`},
		{"lone backslash", `\`, `"\\"`},
		{"other escapes", "\"\n\t\x01", `"\"\n\t\u0001"`},
		{"NUL in a key", map[string]any{"k\x00ey": "v"}, `{"key":"v"}`},
		{"nested", map[string]any{"objectGUID": []any{"\x00\\\x00"}}, `{"objectGUID":["\\"]}`},
		{"number", 42, `42`},
		{"null", nil, `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.val)
			if err != nil {
				t.Fatalf("Marshal(%q) error: %v", tt.val, err)
			}
			if got != tt.want {
				t.Errorf("Marshal(%q) = %s, want %s", tt.val, got, tt.want)
			}
		})
	}
}

func TestMarshalError(t *testing.T) {
	if _, err := Marshal(make(chan int)); err == nil {
		t.Error("Marshal(chan) returned no error")
	}
}

// TestMarshalBinaryValues checks Marshal on random binary values, like the objectGUID,
// objectSid and thumbnailPhoto attributes of Active Directory users. Decoding the output
// must give the value json.Marshal gives, minus its NULs. A \u0000 left in the output
// decodes to a NUL, so that also checks that none is left.
func TestMarshalBinaryValues(t *testing.T) {
	src := rand.NewChaCha8([32]byte{7, 6, 3})
	r := rand.New(src) //nolint:gosec // deterministic test data, not security
	for _, size := range []int{1, 2, 3, 16, 28, 4096} {
		for range 2000 {
			// any bytes (including ones that aren't valid UTF-8), with half of them made NULs
			// and backslashes, the bytes that matter here
			b := make([]byte, size)
			if _, err := src.Read(b); err != nil {
				t.Fatal(err)
			}
			for j := range b {
				switch r.IntN(4) {
				case 0:
					b[j] = 0
				case 1:
					b[j] = '\\'
				}
			}
			val := map[string]any{"attributes": map[string]any{string(b): []any{string(b)}}}

			got, err := Marshal(val)
			if err != nil {
				t.Fatalf("Marshal(%q) error: %v", b, err)
			}
			var decoded any
			if err := json.Unmarshal([]byte(got), &decoded); err != nil {
				t.Fatalf("Marshal(%q) = %s, which isn't valid JSON: %v", b, got, err)
			}
			if want := withoutNuls(t, val); !reflect.DeepEqual(decoded, want) {
				t.Fatalf("Marshal(%q) = %s, which decodes to %q, want %q", b, got, decoded, want)
			}
		}
	}
}

// withoutNuls returns val as json.Marshal and json.Unmarshal give it back, with the NULs
// removed from its strings and keys.
func withoutNuls(t *testing.T, val any) any {
	t.Helper()
	b, err := json.Marshal(val)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	var strip func(any) any
	strip = func(v any) any {
		switch v := v.(type) {
		case string:
			return strings.ReplaceAll(v, "\x00", "")
		case []any:
			for i := range v {
				v[i] = strip(v[i])
			}
			return v
		case map[string]any:
			m := make(map[string]any, len(v))
			for k, e := range v {
				m[strings.ReplaceAll(k, "\x00", "")] = strip(e)
			}
			return m
		}
		return v
	}
	return strip(v)
}
