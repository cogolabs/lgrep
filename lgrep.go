package lgrep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/template"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

var (
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
func NewLGrep(endpoint string) (lg LGrep, err error) {
	lg = LGrep{Endpoint: endpoint}
	lg.Client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return lg, err
}

// SimpleSearch returns the last `count` occurrences of the matching
// search in descending newness.
func (l LGrep) SimpleSearch(q string, count int) (docs []*json.RawMessage, err error) {
	if q == "" {
		return docs, ErrEmptySearch
	}
	docs = make([]*json.RawMessage, 0)
	search, qDebug := l.NewSearch()
	search = SearchWithLucene(search, q).Size(count)
	search = SortByTimestamp(search, SortDesc)

	if l.Debug {
		qDebug(os.Stderr)
	}

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

// FormatSources templates sources into strings for output
func (l LGrep) FormatSources(sources []*json.RawMessage, format string) (msgs []string, err error) {

	tmpl, err := template.New("format").Option("missingkey=zero").Parse(format)
	if err != nil {
		return msgs, errors.Annotate(err, "Format template invalid")
	}
	for i := range sources {
		var data map[string]interface{}
		err = json.Unmarshal(*sources[i], &data)
		if err != nil {
			log.Error(errors.Annotate(err, "Error unmarshalling source"))
			continue
		}
		var buf bytes.Buffer
		err = tmpl.Execute(&buf, data)
		if err != nil {
			log.Error(errors.Annotate(err, "Error templating source"))
			continue
		}
		msgs = append(msgs, string(bytes.TrimSpace(buf.Bytes())))
	}
	return msgs, nil
}
