package lgrep

import (
	"testing"
)

func TestBuildURL(t *testing.T) {
	expectations := map[string]SearchOptions{
		"/_validate/query":                     {},
		"/journald-2016.05.08/_validate/query": {Index: "journald-2016.05.08"},
		"/_all/journald/_validate/query":       {Type: "journald"},
	}

	for expect, spec := range expectations {
		path, _, err := spec.buildURL("_validate/query")
		if err != nil {
			t.Fatal(err)
		}
		if path != expect {
			t.Errorf("URL expected to be %s, was: %s", expect, path)
		}
	}
}
