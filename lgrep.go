package lgrep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"text/template"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

type LGrep struct {
	// Client is the backing interface that searches elasticsearch
	*elastic.Client
	// Endpoint to use when working with Elasticsearch
	Endpoint string
	Debug    bool
}

// NewLGrep client
func NewLGrep(endpoint string) (lg LGrep, err error) {
	lg = LGrep{}
	lg.Client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return lg, err
}

// SimpleSearch returns the last `count` occurrences of the matching
// search in descending newness.
func (l LGrep) SimpleSearch(q string, count int) (docs []*json.RawMessage, err error) {
	docs = make([]*json.RawMessage, 0)
	source := elastic.NewSearchSource()
	search := SearchWithLucene(l.Client.Search().SearchSource(source), q).Size(count)
	search = SortByTimestamp(search, SortDesc)

	// Debug the query that's produced by the search parameters
	if l.Debug {
		queryMap, err := source.Source()
		if err == nil {
			queryJSON, err := json.MarshalIndent(queryMap, "> ", "  ")
			if err == nil {
				fmt.Fprintf(os.Stderr, "> %s\n", queryJSON)
			}
		}
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

// func (l LGrep) FormatMessages(sources []*json.RawMessage) (msgs []string, err error) {
// 	errCount := 0
// 	type message struct {
// 		Message string `json:"message"`
// 	}
// 	for mid := range sources {
// 		var m message
// 		marshallErr := json.Unmarshal(*sources[mid], &m)
// 		if marshallErr != nil {
// 			errCount++
// 			continue
// 		}
// 		msgs = append(msgs, m.Message)
// 	}
// 	if errCount > 0 {
// 		err = fmt.Errorf("%d errors occurred during json parsing of source", errCount)
// 	}
// 	return msgs, err
// }

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
		msgs = append(msgs, buf.String())
	}
	return msgs, nil
}
