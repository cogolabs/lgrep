package lgrep

func LuceneSearchQuery(query string, count int, fields ...string) (source interface{}, err error) {
	type S map[string]interface{}
	var search S

	search = S{
		"size": count,
		"query": S{
			"filtered": S{
				"query": S{
					"query_string": S{
						// Expand wildcards
						"analyze_wildcard": true,
						// Lucene query to be parsed by elasticsearch
						"query": query,
					},
				},
			},
		},
		// Sort by either the
		"sort": []S{
			{
				"@timestamp": S{
					// Allow the field to fail to be ordered
					"unmapped_type": "boolean",
					"order":         "desc",
				},
			},

			{
				"date": S{
					// Allow the field to fail to be ordered
					"unmapped_type": "boolean",
					"order":         "desc",
				},
			},
		},
	}
	return search, nil
}
