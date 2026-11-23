// Package jsonb marshals column values to JSON text that Postgres accepts as jsonb input.
//
// Postgres rejects the \u0000 escape in jsonb ("unsupported Unicode escape sequence"),
// because jsonb stores strings as text and text can't hold a NUL character. json.Marshal
// writes a NUL as \u0000, so a value with a NUL anywhere in it (common in binary values,
// such as Active Directory's objectGUID) fails the whole query. Marshal removes NULs from
// the output instead.
package jsonb

import (
	"encoding/json"
	"strings"
)

const nulEscape = `\u0000`

// Marshal returns val as JSON text with every NUL character removed from its strings and keys.
func Marshal(val any) (string, error) {
	b, err := json.Marshal(val)
	if err != nil {
		return "", err
	}
	return stripNulEscapes(string(b)), nil
}

// stripNulEscapes removes each \u0000 escape from s, which must be JSON from json.Marshal.
//
// A backslash in JSON always starts an escape, so s is read an escape at a time rather than
// searched for \u0000. A search would also match the \u0000 in \\u0000, which is an escaped
// backslash followed by the text u0000: the value holds the six characters \u0000, not a NUL.
// Reading an escape at a time also finds a \u0000 that comes right after another \u0000 and an
// escaped backslash (\u0000\\\u0000, from the bytes 00 5C 00 in a binary value).
func stripNulEscapes(s string) string {
	if !strings.Contains(s, nulEscape) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if strings.HasPrefix(s[i:], nulEscape) {
			i += len(nulEscape) - 1
			continue
		}
		// copy the escape's two characters, so its second one isn't read as the start of another
		b.WriteByte(c)
		if i+1 < len(s) {
			i++
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
