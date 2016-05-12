package lgrep

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

const (
	MaxSearchSize = 10000
	scrollChunk   = 300
)

// SearchStream is a stream of results that manages the execution and
// consumption of that stream.
type SearchStream struct {
	// Results is a channel of results that are read from the server.
	Results chan Result
	// Errors is a channel of errors that are encountered.
	Errors chan error

	// control holds internal variables that are used to control the
	// stream workers.
	control struct {
		*sync.WaitGroup
		sync.Mutex
		stopped bool
		quit    chan struct{}
	}
}

// Wait ensures that the stream has cleaned up after reading all of
// the stream, this should be called after reading the stream in its
// entirety.
func (s SearchStream) Wait() {
	s.control.Lock()
	defer s.control.Unlock()

	s.control.Wait()
}

// Quit instructs the stream to close down cleanly early blocking
// until that happens, this function is safe to call several times.
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

// Results reads the entire stream into memory and returns the results
// that were read, this exits immediately on any error that is
// encountered.
func (s SearchStream) All() (results []Result, err error) {
	resultFn := func(r Result) error {
		results = append(results, r)
		return nil
	}
	// Exit immediately on error!
	errFn := func(err error) error { return err }
	return results, s.Each(resultFn, errFn)
}

// Each executes a function with each result that is read from the
// channel, resultFn and errFn are called when messages are read from
// their respective messages are recieved. If errFn or resultFn
// returns an error, the stream is shutdown.
func (s SearchStream) Each(resultFn func(Result) error, errFn func(error) error) (err error) {
stream:
	for {
		select {
		case streamErr, ok := <-s.Errors:
			if streamErr == nil && !ok {
				continue
			}
			err = errFn(streamErr)
			if err == nil {
				continue
			}
			log.Debug("Error encountered, stopping any ongoing search")
			s.Quit()
			break stream

		case result, ok := <-s.Results:
			if result == nil && !ok {
				break stream
			}
			err = resultFn(result)
			if err != nil {
				break stream
			}
		}
	}
	log.Debug("Waiting for stream to clean up")
	s.Wait()

	return err
}

// execute runs the search and accommodates any necessary work to
// ensure the search is executed properly.
func (l LGrep) execute(search *elastic.SearchService, query elastic.Query, spec SearchOptions) (stream *SearchStream, err error) {
	stream = &SearchStream{
		Results: make(chan Result, 93),
		Errors:  make(chan error, 1),
	}
	stream.control.quit = make(chan struct{}, 1)
	stream.control.WaitGroup = &sync.WaitGroup{}

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
	defer stream.control.Done()

	var (
		nextScrollID string
		discardID    chan string
		count        int
	)
	discardID = make(chan string, 5)

	go func() {
		stream.control.Add(1)
		defer stream.control.Done()

		for {
			select {
			case scrollId, ok := <-discardID:
				if !ok {
					return
				}
				if scrollId == "" {
					continue
				}
				log.Debugf("Clearing scroll id: %s", scrollId[:10])
				clear := l.Client.ClearScroll(scrollId)
				_, err := clear.Do()
				if err != nil {
					log.Warnf("Error clearing scroll %s.", scrollId[:10])
				}
			}
		}
		log.Debug("Scroll keeper closing down")
	}()

	defer close(stream.Results)
	defer close(stream.Errors)
	defer close(discardID)

scrollLoop:
	for {
		if nextScrollID != "" {
			log.Debugf("Fetching next page using scroll id %s", nextScrollID[:10])
			scroll.ScrollId(nextScrollID)
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
				stream.Errors <- errors.Annotate(err, "Error scrolling results")
			}
			break scrollLoop
		}

		if results.ScrollId != "" {
			if results.ScrollId != nextScrollID {
				log.Debug("More results at scrollId: ", results.ScrollId[:10])
				discardID <- nextScrollID
				nextScrollID = results.ScrollId
			}
		}

		for _, hit := range results.Hits.Hits {
			result, err := extractResult(hit, spec)
			if err != nil {
				stream.Errors <- err
			}
			select {
			case <-stream.control.quit:
				log.Debug("Stream instructed to quit")
				break scrollLoop
			case stream.Results <- result:
				count++
			}
			if count == spec.Size {
				log.Debug("Scroll streamed the required amount of results, begin shutdown")
				break scrollLoop
			}
		}
	}
	if nextScrollID != "" {
		discardID <- nextScrollID
	}
	log.Debug("Scroll execution complete, please stream.Wait() for cleanup.")
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
