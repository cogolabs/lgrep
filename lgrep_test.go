package lgrep

import (
	"testing"
)

var TEST_ENDPOINT = "http://localhost:9200"

func TestSearch(t *testing.T) {
	l, err := New(TEST_ENDPOINT)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}
	docs, err := l.SimpleSearch("*", "", 10)
	if err != nil {
		t.Fatalf("Error running search: %s", err)
	}
	if len(docs) != 10 {
		t.Error("Search should have retrieved 10 docs as specified")
	}
}

func TestSearchFormat(t *testing.T) {
	expected := "network"
	l, err := New(TEST_ENDPOINT)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}
	docs, err := l.SimpleSearch("type:"+expected, "", 1)
	if err != nil {
		t.Fatalf("Error running search: %s", err)
	}
	if len(docs) != 1 {
		t.Fatalf("Search should have retrieved 10 docs as specified")
	}
	msgs1, err := FormatSources(docs, "{{.type}}")
	if err != nil {
		t.Fatal(err)
	}
	msgs2, err := FormatSources(docs, ".type")
	if err != nil {
		t.Fatal(err)
	}
	if !(msgs1[0] == msgs2[0] && msgs1[0] == expected) {
		t.Fatalf("Should have all had the same type! (m1: %s, ms2: %s, exp: %s)", msgs1[0], msgs2[0], expected)
	}
}
