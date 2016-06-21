package lgrep

import (
	"encoding/json"
	"net/url"
	"strings"

	"gopkg.in/olivere/elastic.v3"
	"gopkg.in/olivere/elastic.v3/uritemplates"
)

var (
	sortAsc  = true
	sortDesc = false
	// SortAsc sorts the search results with the field ascending.
	SortAsc = &sortAsc
	// SortDesc sorts the search results with the field descending.
	SortDesc = &sortDesc
)

// Searcher is any service that provides a means to execute a query.
type Searcher interface {
	Do() (*elastic.SearchResult, error)
}

// QueryMap is a type of map specifically for use as a query that
// satisfies the elastic.Query interface.
type QueryMap map[string]interface{}

// Source returns the raw source of the query itself - see
// elastic.Query interface.
func (q QueryMap) Source() (interface{}, error) {
	return q, nil
}

// QueryMapFromJSON transforms JSON blobs into a QueryMap for querying.
func QueryMapFromJSON(data []byte) (qm QueryMap, err error) {
	qm = make(QueryMap)
	err = json.Unmarshal(data, &qm)
	return qm, err
}

// SortByTimestamp adds the conventional timestamped fields to the
// search query.
func SortByTimestamp(s *elastic.SearchService, asc bool) *elastic.SearchService {
	for _, f := range []string{"@timestamp", "date"} {
		sort := elastic.NewFieldSort(f)
		sort = sort.UnmappedType("boolean")
		if asc {
			sort = sort.Asc()
		} else {
			sort = sort.Desc()
		}
		s.SortBy(sort)
	}
	return s
}

// SearchWithLucene transforms the textual query into the necessary
// structure to search logstash data.
func SearchWithLucene(s *elastic.SearchService, q string) *elastic.SearchService {
	lucene := elastic.NewQueryStringQuery(q).AnalyzeWildcard(true)
	return s.Query(elastic.NewConstantScoreQuery(lucene))
}

// SearchOptions is used to apply provided options to a search that is
// to be performed.
type SearchOptions struct {
	// Size is the number of records to be returned.
	Size int
	// Index is a single index to search
	Index string
	// Indicies are the indicies that are to be searched
	Indices []string
	// SortTime causes the query to be sorted by the appropriate
	// timestamp field
	SortTime *bool
	// Fields indicates that search results should be limited to the
	// specified field.
	Fields []string
	// Type is the type of document that the search should be limited to.
	Type string
	// Types are the types of documents that should be searched.
	Types []string
	// QueryDebug prints out the resulting query on the console if set
	QueryDebug bool
	// QuerySkipValidate causes the query to be submitted to the server
	// without a pre-validation step.
	QuerySkipValidate bool
	// RawResult will cause results to contain the entire returned hit.
	RawResult bool
}

// buildURL generates the url parts that are appropriate to the
// endpoint and specifciation. Adapted from
// elastic.SearchService.buildURL which is private - we require this
// to submit a query for the _validate endpoint.
func (s SearchOptions) buildURL(endpoint string) (path string, params url.Values, err error) {
	var indices []string
	var types []string
	if s.Index != "" {
		indices = append(indices, s.Index)
	}
	indices = append(indices, s.Indices...)
	if s.Type != "" {
		types = append(types, s.Type)
	}
	types = append(types, s.Types...)

	if len(indices) > 0 && len(types) > 0 {
		path, err = uritemplates.Expand("/{index}/{type}/", map[string]string{
			"index": strings.Join(indices, ","),
			"type":  strings.Join(types, ","),
		})
	} else if len(indices) > 0 {
		path, err = uritemplates.Expand("/{index}/", map[string]string{
			"index": strings.Join(indices, ","),
		})
	} else if len(types) > 0 {
		path, err = uritemplates.Expand("/_all/{type}/", map[string]string{
			"type": strings.Join(types, ","),
		})
	} else {
		path = "/"
	}
	path += endpoint
	if err != nil {
		return "", params, err
	}
	return path, url.Values{}, err
}

// configureSearch applies the options given in the search
// specification to an already instaniated search.
func (s SearchOptions) configureSearch(search *elastic.SearchService) {
	if s.Size != 0 {
		search.Size(s.Size)
	}
	if s.Index != "" {
		search.Index(s.Index)
	}
	if len(s.Indices) != 0 {
		search.Index(s.Indices...)
	}
	if s.SortTime != nil {
		SortByTimestamp(search, *s.SortTime)
	}
	if len(s.Fields) != 0 {
		fsc := elastic.NewFetchSourceContext(true)
		fsc.Include(s.Fields...)
		search.FetchSourceContext(fsc)
	}
}

// configureScroll applies the options given in the search
// specification to an already instaniated scroll.
func (s SearchOptions) configureScroll(scroll *elastic.ScrollService) {
	if s.Size != 0 {
		scroll.Size(s.Size)
	}
	if s.Index != "" {
		scroll.Index(s.Index)
	}
	if len(s.Indices) != 0 {
		scroll.Index(s.Indices...)
	}
}
