package test

import (
	"testing"

	"github.com/cogolabs/lgrep"
)

// https://github.com/cogolabs/lgrep/issues/9
func TestIssue9(t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
	spec := &lgrep.SearchOptions{Index: "inbound-*", Size: 1}
	docs, err := l.SimpleSearch("flags.oddfromtld:true", spec)
	if len(docs) != 1 {
		t.Fatal("Didn't return a single document from the search.")
	}

	msgs, err := lgrep.Format(docs, "{{.route.fromdomain}}")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatal("No formatted messages were returned")
	}
	if len(msgs[0]) == 0 {
		t.Fatalf("Formatted message was empty '%s'", msgs[0])
	}
}
