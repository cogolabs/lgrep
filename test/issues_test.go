package test

import (
	"testing"

	"github.com/cogolabs/lgrep"
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

const (
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// https://github.com/cogolabs/lgrep/issues/9
func TestIssue9(t *testing.T) {
	l, err := lgrep.New(TestEndpoint)
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

	if len(msgs) == 0 {
		t.Fatal("No formatted messages were returned")
	}

	var (
		expected string
	)

	doc, err := docs[0].Map()
	if err != nil {
		t.Fatal(err)
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

func TestIssue11(t *testing.T) {
	tooLargeSize := lgrep.MaxSearchSize + 1
	l, err := lgrep.New(TestEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	results, err := l.SimpleSearch("*", &lgrep.SearchOptions{Size: tooLargeSize, Index: "journald-*", Fields: []string{"host"}})
	if err != nil {
		eserr, _ := errors.Cause(err).(*elastic.Error)
		t.Fatalf("%q\n", eserr.Details)
		t.Fatalf("Error retrieving %d results: %s: %#v", tooLargeSize, err, err)
	}
	if len(results) != tooLargeSize {
		t.Fatalf("Did not return requested amount %d, actual %d", tooLargeSize, len(results))
	}
}
