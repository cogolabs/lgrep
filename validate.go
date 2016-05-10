package lgrep

import (
	"encoding/json"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

var (
	// unvalidatableKeys removes keys that cannot be validated via the API.
	unvalidatableKeys = []string{"_source", "size", "sort"}
	// ErrInvalidQuery indicates that the provided query was not
	// validated by Elasticsearch.
	ErrInvalidQuery = errors.New("Invalid search query")
	// ErrInvalidQuery indicates that the provided query was not
	// validated by Elasticsearch.
	ErrInvalidLuceneSyntax = errors.New("Invalid Lucene syntax - see http://localhost/goto/syntax")
	// ErrInvalidIndex indicates that a query was attempted on a non-existent index or index pattern.
	ErrInvalidIndex = errors.New("Invalid query on unknown index")
)

// ValidationResponse is the Elasticsearch validation result payload.
type ValidationResponse struct {
	Valid  bool
	Shards struct {
		Total      int
		Successful int
		Failed     int
	} `json:"_shards"`
	Explanations []ValidationExplanation
}

// ValidationExplanation is a per-index explanation of a invalid query
// validation result.
type ValidationExplanation struct {
	Index   string
	Valid   bool
	Message string `json:"error"`
	Error   error  `json:"-"`
}

func (l LGrep) validate(query interface{}, spec SearchOptions) (result ValidationResponse, err error) {
	resp, err := l.validateBody(query, spec)
	if err != nil {
		message := err.Error()
		if strings.Contains(message, "index_not_found_exception") {
			return result, ErrInvalidIndex
		}
		return result, err
	}

	result.Explanations = make([]ValidationExplanation, 0)
	err = json.Unmarshal(resp.Body, &result)
	if err != nil {
		return result, err
	}
	if result.Valid {
		return result, nil
	}

	errs := make(map[string]error)

	for i := range result.Explanations {
		exp := result.Explanations[i]
		exp.Error = parseValidationError(exp.Message, exp.Index)
		errs[exp.Error.Error()] = exp.Error
	}

	if len(errs) == 1 {
		for _, e := range errs {
			err = e
		}
		return result, err
	}

	return result, ErrInvalidQuery
}

func (l LGrep) validateBody(query interface{}, spec SearchOptions) (response *elastic.Response, err error) {
	path, params, err := spec.buildURL("_validate/query")
	if err != nil {
		return response, err
	}
	switch v := query.(type) {
	case elastic.SearchSource:
		query, _ = v.Source()
	case *elastic.SearchSource:
		query, _ = v.Source()
	case json.RawMessage:
		query = &v
	default:
		query = v
	}
	var queryMap map[string]interface{}
	data, err := json.Marshal(query)
	if err != nil {
		return response, errors.Errorf("Error during validation prep [0]: %s", err)
	}

	err = json.Unmarshal(data, &queryMap)
	if err != nil {
		return response, errors.Errorf("Error during validation prep [1]: %s", err)
	}

	for _, key := range unvalidatableKeys {
		delete(queryMap, key)
	}

	params.Set("explain", "true")
	log.Debugf("Validating query at '%s?%s'", path, params.Encode())

	return l.Client.PerformRequest("GET", path, params, queryMap)
}

func parseValidationError(msg string, index string) (err error) {
	if msg == "" {
		return nil
	}
	// Only error as lucene when raw json isn't being used.
	if strings.Contains(msg, `Cannot parse`) {
		return ErrInvalidLuceneSyntax
	}
	if index != "" {
		return errors.New(strings.Replace(msg, "["+index+"]", "", 1))
	}
	return errors.New(msg)
}
