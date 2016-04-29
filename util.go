package lgrep

import (
	"bytes"
	"io"
	"strings"
	"unicode"
)

var (
	openBrace  = []byte{'{', '{'}
	closeBrace = []byte{'}', '}'}
)

// CurlyFormat turns a simple jq-like format string into a proper
// text/template parsable format (.field1 => {{.field1}})
func CurlyFormat(str string) (formattable string) {

	if strings.Contains(str, "{{") && strings.Contains(str, "}}") {
		return str
	}

	var dst bytes.Buffer
	src := bytes.NewBufferString(str)
	// When consuming the token, this is true, reading space - false
	inToken := false
	for {
		r, _, err := src.ReadRune()
		if err == io.EOF {
			// If the end was reached and it was a token then
			if inToken {
				dst.Write(closeBrace)
			}
			break
		}
		if err != nil {
			return str
		}

		// Only write the opening brace if its the first one for the token
		if r == rune('.') && !inToken {
			dst.Write(openBrace)
			inToken = true
		}

		// Consume spaces until next token
		if unicode.IsSpace(r) {
			if inToken {
				dst.Write(closeBrace)
			}
			inToken = false
		}

		dst.WriteRune(r)
	}

	return dst.String()
}

// IsRawFormat determines if the specified format string is asking for
// a raw JSON output string.
func IsRawFormat(str string) bool {
	if str == "." || str == "{{.}}" {
		return true
	}
	// If the string AT ALL contains the raw output token then the
	// predicate will indicate that its really a raw format string.
	return strings.Contains(str, "{{.}}")
}
