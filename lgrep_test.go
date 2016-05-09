package lgrep

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
	"testing"

	log "github.com/Sirupsen/logrus"
)

const (
	testJSONQueryPath = "./test/test_json_query.json"
)

var (
	testJSONQuery json.RawMessage
)

func init() {
	data, err := ioutil.ReadFile(testJSONQueryPath)
	if err != nil {
		log.Fatalf("Could not read test query from json file '%s'", testJSONQueryPath)
	}
	testJSONQuery = json.RawMessage(data)
	log.SetLevel(log.DebugLevel)
	var _ = log.Logger{}
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

func TestValidateQuery(t *testing.T) {
	var (
		Valid    error
		AnyError error
	)
	AnyError = errors.New("any")

	expectations := []struct {
		spec    SearchOptions
		search  string      // lucene search
		query   interface{} // json search
		invalid error       // expect to be invalid
		desc    string
	}{
		// Valid searches
		{SearchOptions{Index: "*-*", Type: "journald"}, "*", nil, Valid,
			"*-* pattern and * search with type"},
		{SearchOptions{Index: "*-*"}, "*", nil, Valid,
			"*-* pattern and * search with no type"},
		{SearchOptions{}, "*", nil, Valid,
			"loose * query"},
		{SearchOptions{Type: "journald"}, "", testJSONQuery,
			Valid,
			"using valid json"},

		// Strange but true cases
		{SearchOptions{Type: "nonexistent"}, "*", nil, Valid,
			"querying a nonexistent type"},

		// Invalid searches
		{SearchOptions{Index: "nonexistent"}, "*", nil, ErrInvalidIndex,
			"querying nonexistent index"},
		{SearchOptions{}, "", []byte(`{]`), AnyError,
			"using bad json"},
		{SearchOptions{}, "", []byte(`{"key": "value"}`), ErrInvalidQuery,
			"using incorrect query properties"},
		{SearchOptions{}, "", []byte(`{}`), ErrInvalidQuery,
			"using empty json"},
	}

	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatal(err)
	}

	log.SetLevel(log.DebugLevel)

	for _, testcase := range expectations {
		var result ValidationResponse
		explain := func() {

			repeat := 0
			for i, ex := range result.Explanations {
				if i != 0 {
					last := strings.Split(result.Explanations[i].Error, " ")[1]
					this := strings.Split(result.Explanations[i-1].Error, " ")[1]
					if last == this {
						repeat += 1
						if i == len(result.Explanations)-1 {
							t.Logf("\tAbove message repeated %d times for different indices.", repeat)

						}
						continue
					}
					repeat = 0
				}
				if !ex.Valid {
					t.Logf("\t%s", ex.Error)
				}
			}
		}
		search, source := l.NewSearch()
		// Lucene search specified for case
		if testcase.search != "" {
			SearchWithLucene(search, testcase.search)
			result, err = l.validate(source, testcase.spec)
		} else if testcase.query != nil {
			result, err = l.validate(testcase.query, testcase.spec)
		}

		if err != nil {
			// Expected some kind of issue
			if testcase.invalid == AnyError {
				continue
			}
			// Expected errors
			if err == testcase.invalid {
				continue
			}
			// Expected error but its not the one returned
			if testcase.invalid != nil {
				t.Errorf("Unexpected error during validation: %s", err)
				explain()
				continue
			}

			// Otherwise, it was an error, and it wasn't supposed to be.
			if testcase.desc != "" {
				t.Errorf("Query %s was expected to be valid: %s", testcase.desc, err)
				explain()
			} else {
				t.Error("Invalid query error for valid query: %s", err)
				explain()
			}
			continue
		}

		// The testcase was supposed to be invalid but the call didn't
		// return any errors.
		if testcase.invalid != Valid {
			if testcase.desc != "" {
				t.Errorf("Expected %s to be invalid, but no error was encountered.", testcase.desc)
			} else {
				t.Error("Expected this query to be invalid, but no error was encountered.")
			}
			continue
		}
	}
}
