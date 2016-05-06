package test

import (
	"encoding/json"
	"fmt"
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
	fmt.Println(msgs)
	if len(msgs) == 0 {
		t.Fatal("No formatted messages were returned")
	}

	var (
		doc      map[string]interface{}
		expected string
	)
	err = json.Unmarshal(*docs[0], &doc)
	if err != nil {
		t.Fatalf("Error parsing the json from document: %s", err)
	}
	if val, ok := doc["route"]; ok {
		if route, ok := val.(map[string]interface{}); ok {
			if val, ok := route["fromdomain"]; ok {
				if fromdomain, ok := val.(string); ok {
					expected = fromdomain
				}
			}
		}
	}
	if expected == "" {
		t.Fatal("Not sure what the expected value should be")
	}

	if msgs[0] != expected {
		t.Fatalf("Formatted message '%s' was not expected '%s'", msgs[0], expected)
	}
}
