package lgrep

import (
	"fmt"
	"testing"

	"gopkg.in/olivere/elastic.v3"
)

var TEST_ENDPOINT = "http://localhost:9200"

func TestClient(t *testing.T) {

}

func TestQuery(t *testing.T) {
	q := elastic.NewQueryStringQuery(`myfield:"is awesome" AND hello`)
	q.AnalyzeWildcard(true)
	source, _ := q.Source()
	fmt.Println(source)
	t.Fail()
}
