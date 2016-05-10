package lgrep

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

var (
	// ErrEmptySearch is returned when an empty query is given.
	ErrEmptySearch = errors.New("Empty search query, not submitting.")
	// ErrInvalidQuery indicates that the provided query was not
	// validated by Elasticsearch.
	ErrInvalidQuery = errors.New("Invalid search query")
	// ErrInvalidQuery indicates that the provided query was not
	// validated by Elasticsearch.
	ErrInvalidLuceneSyntax = errors.New("Invalid Lucene syntax - http://localhost/goto/syntax")
	// ErrInvalidIndex indicates that a query was attempted on a non-existent index or index pattern.
	ErrInvalidIndex = errors.New("Invalid query on unknown index")
	// DefaultSpec provides a reasonable default search specification.
	DefaultSpec = SearchOptions{Size: 100, SortTime: SortDesc}
)

// LGrep holds state and configuration for running queries against the
type LGrep struct {
	// Client is the backing interface that searches elasticsearch
	*elastic.Client
	// Endpoint to use when working with Elasticsearch
	Endpoint string
}

// New creates a new lgrep client.
func New(endpoint string) (lg LGrep, err error) {
	lg = LGrep{Endpoint: endpoint}
	lg.Client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return lg, err
}

// SimpleSearch runs a lucene search configured by the SearchOption
// specification.
func (l LGrep) SimpleSearch(q string, spec *SearchOptions) (results []Result, err error) {
	if q == "" {
		return results, ErrEmptySearch
	}
	results = make([]Result, 0)
	search, source := l.NewSearch()
	search = SearchWithLucene(search, q)
	if spec != nil {
		// If user wants 0 then they're really not looking to get any
		// results, don't execute.
		if spec.Size == 0 {
			return results, err
		}
	} else {
		spec = &DefaultSpec
	}
	spec.apply(search)

	// Spit out the query that will be sent.
	if spec.QueryDebug {
		query, err := source.Source()
		if err != nil {
			log.Error(errors.Annotate(err, "Error generating query source, may indicate further issues."))
		}
		printQueryDebug(os.Stderr, query)
	}

	log.Debug("Submitting search request..")
	res, err := search.Do()
	if err != nil {
		return results, errors.Annotatef(err, "Search returned with error")
	}
	return consumeResults(res, spec)
}

// SearchWithSource may be used to provide a pre-contstructed json
// query body when a query cannot easily be formed with the available
// methods. The applied SearchOptions specification *is not fully
// compatible* with a manually crafted query body but some options are
// - see SearchOptions for any caveats.
func (l LGrep) SearchWithSource(query interface{}, spec *SearchOptions) (results []Result, err error) {
	search, _ := l.NewSearch()
	if spec != nil {
		// If user wants 0 then they're really not looking to get any
		// results, don't execute.
		if spec.Size == 0 {
			return results, err
		}
	} else {
		spec = &DefaultSpec
	}
	spec.apply(search)
	switch v := query.(type) {
	case json.RawMessage:
		search.Source(&v)
	case []byte:
		data := json.RawMessage(v)
		search.Source(&data)
	case map[string]interface{}:
		search.Source(v)
	default:
		log.Fatalf("SearchWithSource does not support type '%T' at this time.", v)
	}

	if spec.QueryDebug {
		printQueryDebug(os.Stderr, query)
	}

	log.Debug("Submitting search request..")
	res, err := search.Do()
	if err != nil {
		return results, errors.Annotatef(err, "Search returned with error")
	}
	return consumeResults(res, spec)
}

// consumeResultpps ingests the results from the returned data and
// transforms them into Result's.
func consumeResults(res *elastic.SearchResult, spec *SearchOptions) (results []Result, err error) {
	for _, doc := range res.Hits.Hits {
		// Extract the fields that were returned
		if len(spec.Fields) != 0 && len(doc.Fields) != 0 {
			results = append(results, FieldResult(doc.Fields))
			continue
		}
		if doc == nil || doc.Source == nil {
			return results, errors.New("nil document returned")
		}
		results = append(results, SourceResult(*doc.Source))
	}
	return results, nil
}

// SearchTimerange will return occurrences of the matching search in
// the timeframe provided.
func (l LGrep) SearchTimerange(search string, count int, t1 time.Time, t2 time.Time) {

}

// NewSearch initializes a new search object along with a func to
// debug the resulting query to be sent.
func (l LGrep) NewSearch() (search *elastic.SearchService, source *elastic.SearchSource) {
	source = elastic.NewSearchSource()
	search = l.Client.Search().SearchSource(source)

	return search, source
}

// printQueryDebug prints out the formatted JSON query body that will
// be submitted.
func printQueryDebug(out io.Writer, query interface{}) {
	var (
		queryJSON []byte
		err       error
	)

	// json.RawMessage must be passed as a pointer to be Marshaled
	// correctly.
	if raw, ok := query.(json.RawMessage); ok {
		queryJSON, err = json.MarshalIndent(&raw, "q> ", "  ")
	} else {
		queryJSON, err = json.MarshalIndent(query, "q> ", "  ")
	}
	if err == nil {
		fmt.Fprintf(out, "q> %s\n", queryJSON)
	}
}
