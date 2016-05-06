package lgrep

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"text/template"
	"time"
	"unicode"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
)

const (
	normalTSField = "timestamp"
)

var (
	openBrace  = []byte{'{', '{'}
	closeBrace = []byte{'}', '}'}
	// tsPreference determines the fields that are used to extract the
	// timestamp for the document.
	tsPreference = []string{"@timestamp", "date"}
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

// Format templates documents into strings for output
func Format(sources []*json.RawMessage, format string) (msgs []string, err error) {
	// If its raw, cleanup the json and then spit that out
	if IsRawFormat(format) {
		for _, s := range sources {
			msgs = append(msgs, string(bytes.TrimSpace(*s)))
		}
		return msgs, nil
	}

	format = CurlyFormat(format)
	log.Debugf("Using template format: '%s'", format)
	tmpl, err := template.New("format").
		Option("missingkey=zero").Funcs(template.FuncMap{
		// format time - not eff-time, its invulnerable.
		"ftime": strftime,
	}).Parse(format)
	if err != nil {
		return msgs, errors.Annotate(err, "Format template invalid")
	}
	log.Debugf("Formatting %d sources", len(sources))
	for i := range sources {
		var data map[string]interface{}

		err = json.Unmarshal(*sources[i], &data)
		if err != nil {
			return msgs, err
		}

		data = normalizeTS(data)

		var buf bytes.Buffer
		err = tmpl.Execute(&buf, data)
		if err != nil {
			return msgs, err
		}
		msgs = append(msgs, string(bytes.TrimSpace(buf.Bytes())))
	}
	return msgs, nil
}

// Some index used date and others the @timestamp field for the ts
func normalizeTS(data map[string]interface{}) map[string]interface{} {
	// If the ts has already been normalized then don't try to parse
	// this again.
	if ts, hasKey := data["timestamp"]; hasKey {
		if _, isTime := ts.(time.Time); isTime {
			return data
		}
	}

	var normalized bool

	for _, tsField := range tsPreference {
		if val, ok := data[tsField]; ok {
			// If the value is a string then parse that out
			str, ok := val.(string)
			if !ok {
				continue
			}
			ts, err := time.Parse(time.RFC3339, str)
			if err != nil {
				continue
			}
			normalized = true
			data[normalTSField] = ts
			break
		}
	}
	if !normalized {
		log.Debug("Timestamp could not be normalized from data")
	}
	return data
}

// templateFieldTokens extracts the tokens that are used in the
// template.
func templateFieldTokens(t string) (tokens []string) {
	t = CurlyFormat(t)
	matcher := regexp.MustCompile(`{{([^{}]+)}}`)
	matches := matcher.FindAllStringSubmatch(t, -1)
	for _, match := range matches {
		token := match[1]
		tokens = append(tokens, strings.TrimSpace(token))
	}
	return tokens
}

// strftime is a template formatting function
func strftime(format string, d interface{}) string {
	var t time.Time
	switch v := d.(type) {
	case time.Time:
		t = v
	case string:
		return v
	case bool:
		t, _ = time.Parse("2006-01-02 15:04", "1955-11-05 06:00")
	}
	return t.Format(format)
}
