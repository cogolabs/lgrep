package lgrep

import (
	"encoding/json"
	"errors"
	"io/ioutil"
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
		t.Errorf("Search should have retrieved 10 docs as specified, returned %d", len(docs))
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
		t.Fatalf("Search should have retrieved 1 docs as specified, returned %d", len(docs))
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
		t.Fatalf("Search should have retrieved 1 results as specified, returned %d", len(results))
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

func TestRawResultQuery(t *testing.T) {
	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatalf("Client error: %s", err)
	}

	opts := &SearchOptions{Size: 1, RawResult: true}
	results, err := l.SimpleSearch("*", opts)
	if err != nil {
		t.Fatalf("Error performing search: %s", err)
	}
	if len(results) != opts.Size {
		t.Fatalf("Number of results %d was not the expected amount %d.", len(results), opts.Size)
	}
	if hit, ok := results[0].(HitResult); ok {
		if hit.Id == "" {
			t.Fatal("Raw result should have had an ID")
		}
	} else {
		t.Fatalf("Hit (%T) was not a HitResult", hit)
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

		// TODO: Fix validation for raw json files
		// {SearchOptions{Type: "journald"}, "", testJSONQuery,
		// 	Valid,
		// 	"using valid json"},

		// Strange but true cases
		{SearchOptions{Type: "nonexistent"}, "*", nil, Valid,
			"querying a nonexistent type"},

		// Invalid searches
		{SearchOptions{Index: "nonexistent"}, "*", nil, ErrInvalidIndex,
			"querying nonexistent index"},
		{SearchOptions{}, "", []byte(`{]`), AnyError,
			"using bad json"},
		{SearchOptions{}, "", []byte(`{"key": "value"}`), AnyError,
			"using incorrect query properties"},
		{SearchOptions{}, "", []byte(`{}`), AnyError,
			"using empty json"},
		{SearchOptions{}, `NOT`, nil, ErrInvalidLuceneSyntax,
			"just a NOT, invalid lucene syntax"},
	}

	l, err := New(TestEndpoint)
	if err != nil {
		t.Fatal(err)
	}

	for _, testcase := range expectations {
		var result ValidationResponse
		explain := func() {
			repeat := 0
			for i, ex := range result.Explanations {
				if i != 0 && ex.Error != nil {
					last := result.Explanations[i-1]
					if ex.Error.Error() == last.Error.Error() {
						repeat++
						if i == len(result.Explanations)-1 {
							t.Logf("\tAbove message repeated %d times for different indices.", repeat)
						}
						continue
					}
					repeat = 0
				}
				if !ex.Valid && ex.Error != nil {
					t.Logf("\t%s", ex.Error.Error())
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
				t.Errorf("Query %s returned unexpected err: %s", testcase.desc, err)
				explain()
				continue
			}

			// Otherwise, it was an error, and it wasn't supposed to be.
			if testcase.desc != "" {
				t.Errorf("Query %s was expected to be valid: %s", testcase.desc, err)
				explain()
			} else {
				t.Errorf("Invalid query error for valid query: %s", err)
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
