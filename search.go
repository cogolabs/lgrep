package lgrep

import (
	"gopkg.in/olivere/elastic.v3"
)

var (
	sortAsc  = true
	sortDesc = false
	// SortAsc sorts the search results with the field ascending.
	SortAsc = &sortAsc
	// SortDesc sorts the search results with the field descending.
	SortDesc = &sortDesc
)

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
	query := elastic.NewQueryStringQuery(q).AnalyzeWildcard(true)
	return s.Query(query)
}
