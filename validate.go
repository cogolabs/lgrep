package lgrep

import (
	"encoding/json"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
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
	var (
		body     interface{}
		JSONBody *json.RawMessage
	)
	params.Set("explain", "true")
	log.Debugf("Validating query at '%s?%s'", path, params.Encode())

	switch v := query.(type) {
	case elastic.SearchSource:
		body, err = v.Source()
		if err != nil {
			return response, err
		}

	case *elastic.SearchSource:
		body, err = v.Source()
		if err != nil {
			return response, err
		}

	case []byte:
		data := json.RawMessage(v)
		JSONBody = &data
	}
	if body != nil {
		if log.GetLevel() == log.DebugLevel {
			printQueryDebug(os.Stderr, body)
		}
		return l.Client.PerformRequest("GET", path, params, body)
	}

	if JSONBody != nil {
		if log.GetLevel() == log.DebugLevel {
			printQueryDebug(os.Stderr, JSONBody)
		}
		return l.Client.PerformRequest("GET", path, params, JSONBody)
	}

	return nil, errors.New("Cannot validate request body type")
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
