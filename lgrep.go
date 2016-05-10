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
	spec.configureSearch(search)

	// Spit out the query that will be sent.
	if spec.QueryDebug {
		query, err := source.Source()
		if err != nil {
			log.Error(errors.Annotate(err, "Error generating query source, may indicate further issues."))
		}
		printQueryDebug(os.Stderr, query)
	}

	if !spec.QuerySkipValidate {
		_, err := l.validate(source, *spec)
		if err != nil {
			return results, err
		}
	}

	log.Debug("Submitting search request..")
	stream, streamErrs, quit := l.execute(search, source, *spec)

searchStream:
	for {
		select {
		case result, open := <-stream:
			if result != nil {
				results = append(results, result)
			}
			if !open {
				break searchStream
			}
		case err := <-streamErrs:
			if err != nil {
				// Exit on any errors
				ack := make(chan struct{})
				quit <- &ack
				timeout := time.NewTimer(time.Second * 3)
				// Wait for the search to cleanup
				select {
				case <-ack:
					break
				case <-timeout.C:
					timeout.Stop()
					log.Warn("Timed out shutting down search query")
				}
				return results, errors.Annotatef(err, "Search returned with error")
			}
		}
	}

	return results, nil
}

// SearchWithSource may be used to provide a pre-contstructed json
// query body when a query cannot easily be formed with the available
// methods. The applied SearchOptions specification *is not fully
// compatible* with a manually crafted query body but some options are
// - see SearchOptions for any caveats.
func (l LGrep) SearchWithSource(raw interface{}, spec *SearchOptions) (results []Result, err error) {
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
	spec.configureSearch(search)
	var query elastic.Query
	switch v := raw.(type) {
	case json.RawMessage:
		query, err = QueryMapFromJSON(v)
	case []byte:
		data := json.RawMessage(v)
		query, err = QueryMapFromJSON(data)
	case map[string]interface{}:
		query = QueryMap(v)
	default:
		log.Fatalf("SearchWithSource does not support type '%T' at this time.", v)
	}

	if spec.QueryDebug {
		printQueryDebug(os.Stderr, query)
	}

	if !spec.QuerySkipValidate {
		_, err := l.validate(query, *spec)
		if err != nil {
			return results, err
		}
	}

	log.Debug("Submitting search request..")
	res, err := search.Do()
	if err != nil {
		return results, errors.Annotatef(err, "Search returned with error")
	}
	return consumeResults(res, *spec)
}

// execute runs the search and accomodates any necessary work to
// ensure the search is executed properly.
func (l LGrep) execute(search *elastic.SearchService, query elastic.Query, spec SearchOptions) (results chan Result, streamErr chan error, quit chan *chan struct{}) {
	results = make(chan Result, 93)
	streamErr = make(chan error, 1)
	quit = make(chan *chan struct{}, 1)

	var (
		service Searcher = search
		scroll  *elastic.ScrollService
	)

	if spec.Size > 10000 {
		scroll = l.Scroll()
		spec.configureScroll(scroll)
		scroll.Query(query)
		service = scroll
	}

	go func() {
		defer close(results)
		defer close(streamErr)
		var (
			scrolls []string
		)

	searchLoop:
		for {
			result, err := service.Do()
			if err != nil {
				// End of the scroll
				if err == elastic.EOS {
					break
				}
				// Any other error
				streamErr <- err
				break
			}

			for i := range result.Hits.Hits {
				select {
				case ack := <-quit:
					defer func() { *ack <- struct{}{} }()
					break searchLoop
				default:
					result, err := extractResult(result.Hits.Hits[i], spec)
					if err != nil {
						streamErr <- err
					}
					results <- result
				}
			}

			// Gotta scroll!
			if scroll != nil {
				scrolls = append(scrolls, result.ScrollId)
				scroll.ScrollId(scrolls[len(scrolls)-1])
			} else {
				break searchLoop
			}
		}

		if len(scrolls) != 0 {
			log.Debugf("Cleaning up %d scrolls", len(scrolls))
			// Clean up used scrolls
			_, err := l.ClearScroll(scrolls...).Do()
			if err != nil {
				log.Debug("Error cleaning up scroll, they'll expire")
				streamErr <- err
			}
		}
		log.Debug("Exiting execute streamer")
	}()

	return results, streamErr, quit
}

//
func extractResult(hit *elastic.SearchHit, spec SearchOptions) (result Result, err error) {
	if len(spec.Fields) != 0 && len(hit.Fields) != 0 {
		return FieldResult(hit.Fields), nil
	}
	if hit == nil || hit.Source == nil {
		return nil, errors.New("nil document returned")
	}
	return SourceResult(*hit.Source), nil
}

// consumeResults ingests the results from the returned data and
// transforms them into Result's.
func consumeResults(res *elastic.SearchResult, spec SearchOptions) (results []Result, err error) {
	for _, doc := range res.Hits.Hits {
		result, err := extractResult(doc, spec)
		if err != nil {
			return results, err
		}
		results = append(results, result)
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
