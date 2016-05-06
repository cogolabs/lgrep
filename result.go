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
	String() string
}

// FieldResult is returned from a document with fields specified to be
// returned.
type FieldResult map[string]interface{}

func (fr FieldResult) Map() (map[string]interface{}, error) {
	return fr, nil
}
func (fr FieldResult) JSON() ([]byte, error) {
	return json.Marshal(fr)
}

func (fr FieldResult) String() string {
	b, err := fr.JSON()
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// SourceResult is returned when an entire document is requested.
type SourceResult json.RawMessage

func (sr SourceResult) Map() (data map[string]interface{}, err error) {
	err = json.Unmarshal(sr, &data)
	return data, err
}

func (sr SourceResult) JSON() ([]byte, error) {
	return sr, nil
}

func (sr SourceResult) String() string {
	b, err := sr.JSON()
	if err != nil {
		return err.Error()
	}
	return string(b)
}
