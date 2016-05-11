package lgrep

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

const (
	scrollChunk = 100
)

type SearchStream struct {
	Results chan Result
	Errors  chan error

	control struct {
		sync.WaitGroup
		sync.Mutex
		stopped bool
		quit    chan struct{}
	}
}

func (s SearchStream) Quit() {
	log.Debug("Sending stream quit signal")
	s.control.Lock()
	defer s.control.Unlock()
	if s.control.stopped {
		return
	}
	s.control.quit <- struct{}{}
	timeout := time.NewTimer(time.Second * 1)
	stopped := make(chan struct{}, 1)
	go func() { s.control.Wait(); stopped <- struct{}{} }()
	select {
	case <-timeout.C:
	case <-stopped:
	}
	s.control.stopped = true
}

// execute runs the search and accomodates any necessary work to
// ensure the search is executed properly.
func (l LGrep) execute(search *elastic.SearchService, query elastic.Query, spec SearchOptions) (stream *SearchStream, err error) {
	stream = &SearchStream{
		Results: make(chan Result, 93),
		Errors:  make(chan error, 1),
	}
	stream.control.quit = make(chan struct{}, 1)

	if spec.Size > 10000 {
		if spec.Index == "" || (spec.Index == "" && len(spec.Indices) == 0) {
			return nil, errors.New("An index pattern must be given for large requests")
		}
		source, err := query.Source()
		if err != nil {
			return nil, err
		}
		// Remove the size key if possible, if its too large (which at
		// this point it will be if configured by the spec), then the
		// query will need to have the size key removed as that is a
		// specification for how many results from each shard.
		if queryMap, ok := source.(map[string]interface{}); ok {
			qm := QueryMap(queryMap)
			delete(qm, "size")
			query = qm
		}

		scroll := l.Scroll()
		spec.configureScroll(scroll)
		scroll.Size(scrollChunk)
		// Must have been patched for `body = query` otherwise the
		// ScrollService will nest the query further incorrectly.
		scroll.Query(query)
		scroll.KeepAlive("30s")

		go l.executeScroll(scroll, query, spec, stream)
	} else {
		go l.executeSearcher(search, query, spec, stream)
	}

	return stream, nil
}

func (l LGrep) executeScroll(scroll *elastic.ScrollService, query elastic.Query, spec SearchOptions, stream *SearchStream) {
	stream.control.Add(1)
	var (
		nextScrollId string
		lastScrollId string
		count        int
	)

	cleanupScroll := func(id string) {
		if id != "" {
			clear := l.Client.ClearScroll(id)
			_, err := clear.Do()
			if err != nil {
				log.Warnf("Could not clear discarded scroll '%s'", id)
				stream.Errors <- err
			}
		}
	}

	defer cleanupScroll(lastScrollId)
	defer cleanupScroll(nextScrollId)

	defer close(stream.Results)
	defer close(stream.Errors)

	for {
		if lastScrollId != "" {
			log.Debugf("Clearing previous scroll '%s'\n", lastScrollId[:10])
			cleanupScroll(lastScrollId)
			lastScrollId = ""
		}

		if nextScrollId != "" {
			log.Debugf("Preparing this scroll %s", nextScrollId[:10])
			scroll.ScrollId(nextScrollId)
			lastScrollId = nextScrollId
			if count >= spec.Size {
				return
			}
		} else {
			log.Debug("Fetching first page of scroll")
		}

		results, err := scroll.Do()
		if err != nil {
			log.Debug("An error was returned from the scroll")
			if err != elastic.EOS {
				stream.Errors <- err
			}
			return
		}

		if results.ScrollId != "" {
			nextScrollId = results.ScrollId
		}

		log.Debug("Streaming results from the scroll")
		for _, hit := range results.Hits.Hits {
			result, err := extractResult(hit, spec)
			if err != nil {
				stream.Errors <- err
			}
			select {
			case <-stream.control.quit:
				return
			case stream.Results <- result:
				count += 1
			}
			if count == spec.Size {
				return
			}
		}
	}
}

func (l LGrep) executeSearcher(service Searcher, query elastic.Query, spec SearchOptions, stream *SearchStream) {
	// Start worker
	stream.control.Add(1)
	defer stream.control.Done()

	defer close(stream.Results)
	defer close(stream.Errors)

	result, err := service.Do()

	if err != nil {
		stream.Errors <- err
		return
	}

	for i := range result.Hits.Hits {
		select {
		case <-stream.control.quit:
			return
		default:
			result, err := extractResult(result.Hits.Hits[i], spec)
			if err != nil {
				stream.Errors <- err
			}
			stream.Results <- result
		}
	}
}
