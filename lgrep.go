package lgrep

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v3"
)

type LGrep struct {
	// Client is the backing interface that searches elasticsearch
	*elastic.Client
	// Endpoint to use when working with Elasticsearch
	Endpoint string
}

type Client interface {
	SetTimeframe(begin time.Time, end time.Time)
	Search(search string, index string, count int) (docSources []*json.RawMessage, err error)
	Health() (healthy bool, err error)
}

func NewLGrep(endpoint string) (lg LGrep, err error) {
	lg = LGrep{}
	lg.Client, err = elastic.NewClient(elastic.SetURL(endpoint))
	return lg, err
}

func (l LGrep) Messages(sources []*json.RawMessage) (msgs []string, err error) {
	errCount := 0
	type message struct {
		Message string `json:"message"`
	}
	for mid := range sources {
		var m message
		marshallErr := json.Unmarshal(*sources[mid], &m)
		if marshallErr != nil {
			errCount++
			continue
		}
		msgs = append(msgs, m.Message)
	}
	if errCount > 0 {
		err = fmt.Errorf("%d errors occurred during json parsing of source", errCount)
	}
	return msgs, err
}
