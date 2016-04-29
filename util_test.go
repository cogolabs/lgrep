package lgrep

import (
	"testing"
)

func TestCurlyFormat(t *testing.T) {
	examples := [][]string{
		{".one", "{{.one}}"},
		{".one .two", "{{.one}} {{.two}}"},
		{".one {{.two}}", ".one {{.two}}"},
		{".one . .two", "{{.one}} {{.}} {{.two}}"},
	}

	for _, ex := range examples {
		res := CurlyFormat(ex[0])
		if res != ex[1] {
			t.Errorf("CurlyFormat('%s') => '%s' (expected '%s')", ex[0], res, ex[1])
		}
	}
}
