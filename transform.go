package lgrep

const (
	TimestampField = "_ts"
)

// TransformTimestamp parses the date and add a _ts field that may be
// used elsewhere.
func TransformTimestamp(data1 map[string]interface{}) (data2 map[string]interface{}, err error) {
	_, ok := data1["@timestamp"]
	if ok {

	}

	return data1, nil
}
