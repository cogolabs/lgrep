package lgrep

import (
	"encoding/json"
)

// Result is a generic result from a search.
type Result interface {
	// Map turns the result into a map
	Map() (map[string]interface{}, error)
	// JSON turns the result into marshaled JSON
	JSON() ([]byte, error)
	// String formats a result for logging and untemplated output
	// purposes.
	String() string
}

// FieldResult is returned from a document with fields specified to be
// returned.
type FieldResult map[string]interface{}

// Map decodes the FieldResult into a map.
func (fr FieldResult) Map() (map[string]interface{}, error) {
	return fr, nil
}

// JSON encodes the result into a JSON document.
func (fr FieldResult) JSON() ([]byte, error) {
	return json.Marshal(fr)
}

// String formats a result for logging and untemplated output
// purposes.
func (fr FieldResult) String() string {
	b, err := fr.JSON()
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// SourceResult is returned when an entire document is requested.
type SourceResult json.RawMessage

// Map transforms SourceResult into a map from its native format.
func (sr SourceResult) Map() (data map[string]interface{}, err error) {
	err = json.Unmarshal(sr, &data)
	return data, err
}

// JSON encodes the result into a JSON document - in this case a no-op.
func (sr SourceResult) JSON() ([]byte, error) {
	return sr, nil
}

// String formats a result for logging and untemplated output
// purposes.
func (sr SourceResult) String() string {
	b, err := sr.JSON()
	if err != nil {
		return err.Error()
	}
	return string(b)
}
