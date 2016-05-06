package lgrep

import (
	"testing"
)

func init() {
	data, err := ioutil.ReadFile(testJSONQueryPath)
	if err != nil {
		log.Fatalf("Could not read test query from json file '%s'", testJSONQueryPath)
	}
	testJSONQuery = json.RawMessage(data)
	log.SetLevel(log.DebugLevel)
}

var TestEndpoint = "http://localhost:9200"

func TestSearch(t *testing.T) {
	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}
	opts := &SearchOptions{Size: 10}
	docs, err := l.SimpleSearch("*", opts)
	if err != nil {
		t.Fatalf("Error running search: %s", err)
	}
	if len(docs) != 10 {
		t.Error("Search should have retrieved 10 docs as specified")
	}
}

func TestSearchFormat(t *testing.T) {
	expected := "network"
	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}
	opts := &SearchOptions{Size: 1}
	docs, err := l.SimpleSearch("type:"+expected, opts)
	if err != nil {
		t.Fatalf("Error running search: %s", err)
	}
	if len(docs) != 1 {
		t.Fatalf("Search should have retrieved 10 docs as specified")
	}
	msgs1, err := Format(docs, "{{.type}}")
	if err != nil {
		t.Fatal(err)
	}
	msgs2, err := Format(docs, ".type")
	if err != nil {
		t.Fatal(err)
	}
	if !(msgs1[0] == msgs2[0] && msgs1[0] == expected) {
		t.Fatalf("Should have all had the same type! (m1: %s, ms2: %s, exp: %s)", msgs1[0], msgs2[0], expected)
	}
}

func TestSearchFields(t *testing.T) {
	docType := "journald"
	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}
	opts := &SearchOptions{Size: 1, Fields: []string{"type"}}
	results, err := l.SimpleSearch("type:"+docType, opts)
	if err != nil {
		t.Fatalf("Error running search: %s", err)
	}
	if len(results) != 1 {
		t.Fatalf("Search should have retrieved 1 results as specified")
	}
	msgs1, err := Format(results, "{{.type}}")
	if err != nil {
		t.Fatal(err)
	}
	if !(msgs1[0] == docType) {
		t.Fatalf("Should have all had the same type! (m1: %s, type: %s)", msgs1[0], docType)
	}
	if len(msgs1) != 1 {
		t.Errorf("Was only to return a single field: type. %#v", msgs1[0])
	}
}
