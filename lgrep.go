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
)

// LGrep holds state and configuration for running queries against the
type LGrep struct {
	// Client is the backing interface that searches elasticsearch
	*elastic.Client
	// Endpoint to use when working with Elasticsearch
	Endpoint string
	Debug    bool
}

// NewLGrep client
func New(endpoint string) (lg LGrep, err error) {
	lg = LGrep{Endpoint: endpoint}
	lg.Client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return lg, err
}

// SimpleSearch returns the last `count` occurrences of the matching
// search in descending newness.
func (l LGrep) SimpleSearch(q string, index string, count int) (docs []*json.RawMessage, err error) {
	if q == "" {
		return docs, ErrEmptySearch
	}
	docs = make([]*json.RawMessage, 0)
	search, qDebug := l.NewSearch()
	search = SearchWithLucene(search, q).Size(count)
	search = SortByTimestamp(search, SortDesc)
	if index != "" {
		search.Index(index)
	}

	// Spit out the query that will be sent.
	if l.Debug {
		qDebug(os.Stderr)
	}

	// If user wants 0 then they're really not looking to get any
	// results, don't execute.
	if count == 0 {
		return docs, nil
	}
	log.Debug("Submitting search request..")
	res, err := search.Do()
	if err != nil {
		return docs, errors.Annotatef(err, "Search returned with error")
	}
	for _, doc := range res.Hits.Hits {
		docs = append(docs, doc.Source)
	}
	return docs, nil
}

// SearchTimerange will return occurrences of the matching search in
// the timeframe provided.
func (l LGrep) SearchTimerange(search string, count int, t1 time.Time, t2 time.Time) {

}

// NewSearch initializes a new search object along with a func to
// debug the resulting query to be sent.
func (l LGrep) NewSearch() (search *elastic.SearchService, dbg func(wr io.Writer)) {
	source := elastic.NewSearchSource()
	search = l.Client.Search().SearchSource(source)

	// Debug the query that's produced by the search parameters
	dbg = func(wr io.Writer) {
		queryMap, err := source.Source()
		if err == nil {
			queryJSON, err := json.MarshalIndent(queryMap, "> ", "  ")
			if err == nil {
				fmt.Fprintf(wr, "> %s\n", queryJSON)
			}
		}
	}
	return search, dbg
}
