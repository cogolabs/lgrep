package elasticsearch

import (
	"encoding/json"
	"time"

	"gopkg.in/olivere/elastic.v3"
)

// Client offers the API to connect to Elasticsearch
type Client struct {
	client *elastic.Client
}

// Search Elasticsearch using search string and return the source of
// the returned documents.
func (c Client) Search(search string, index string, count int, offset int) (sources []*json.RawMessage, err error) {
	q := elastic.NewQueryStringQuery(search)
	result, err := c.client.Search(index).Query(q).Sort("@timestamp", false).Size(count).Do()
	if err != nil {
		return sources, err
	}

	for _, source := range result.Hits.Hits {
		sources = append(sources, source.Source)
	}

	return sources, err
}

// Health returns the health of the cluster that we are connecting to.
func (c Client) Health() (healthy bool, err error) {
	err = c.client.ClusterHealth().Validate()
	return true, err
}

func (c Client) SetTimeframe(begin time.Time, end time.Time) {
	return
}

func NewClient(endpoint string) (cl Client, err error) {
	cl = Client{}
	cl.client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return cl, err
}
