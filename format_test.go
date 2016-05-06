package lgrep

import (
	"sort"
	"testing"
)

func TestCurlyFormat(t *testing.T) {
	examples := [][]string{
		{".one", "{{.one}}"},
		{".one .two", "{{.one}} {{.two}}"},
		{".one {{.two}}", ".one {{.two}}"},
		{".one . .two", "{{.one}} {{.}} {{.two}}"},
		{".one-one", "{{.one-one}}"},
		{".@timestamp", "{{.@timestamp}}"},
		{`."@timestamp"`, `{{."@timestamp"}}`},
		{".one\t.two", "{{.one}}\t{{.two}}"},
	}

	for _, ex := range examples {
		res := CurlyFormat(ex[0])
		if res != ex[1] {
			t.Errorf("CurlyFormat('%s') => '%s' (expected '%s')", ex[0], res, ex[1])
		}
	}
}

func TestFieldTokens(t *testing.T) {
	testData := map[string][]string{
		"{{.one}} {{.two}}": {".one", ".two"},
		".one .two":         {".one", ".two"},
		".one .two.three":   {".one", ".two.three"},
	}
	for s, expected := range testData {
		tokens := templateFieldTokens(s)
		if len(tokens) != len(expected) {
			t.Errorf("Incorrect number of tokens from '%s': %d != %d", s, len(tokens), len(expected))
			continue
		}
		sort.Strings(tokens)
		sort.Strings(expected)

		for i := range expected {
			if tokens[i] != expected[i] {
				t.Errorf("Extracted tokens from '%s': %s != %s", s, tokens, expected)
				break
			}
		}
	}
}
