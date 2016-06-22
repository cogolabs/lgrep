package lgrep

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

const (
	// MaxSearchSize is the maximum search size that is able to be
	// performed before the search will necessitate a scroll.
	MaxSearchSize = 10000
	scrollChunk   = 100
)

var (
	// EOS is the sentinel value indicating that the end of stream has
	// been reached.
	EOS Result = nil
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
func (s *SearchStream) Wait() {
	s.control.Lock()
	defer s.control.Unlock()

	s.control.Wait()
}

// Quit instructs the stream to close down cleanly early blocking
// until that happens, this function is safe to call several times.
func (s *SearchStream) Quit() {
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

// All reads the entire stream into memory and returns the results
// that were read, this exits immediately on any error that is
// encountered.
func (s *SearchStream) All() (results []Result, err error) {
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
// their respective messages are received. If errFn or resultFn
// returns an error, the stream will shutdown early. The resultFn will
// be passed a nil value when the stream is finished thereby
// indicating the end of the stream.
func (s *SearchStream) Each(resultFn func(Result) error, errFn func(error) error) (err error) {
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
				log.Debug("Stream results dried up, breaking out.")
				err = resultFn(EOS)
				break stream
			}
			err = resultFn(result)
			if err != nil {
				log.Debug("An error occurred with upstream handler, breaking out")
				break stream
			}
		}
	}
	log.Debug("Exiting stream loop, waiting for stream to clean up")
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
	if spec.QueryDebug {
		log.SetLevel(log.DebugLevel)
	}
	stream.control.quit = make(chan struct{}, 1)
	stream.control.WaitGroup = &sync.WaitGroup{}

	if spec.Size > MaxSearchSize {
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
		if scrollChunk <= 0 {
			log.Fatal("YOU WILL DESTROY LOGSTASH, ABORTING.")
		}
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
		resultCount  int
	)
	discardID = make(chan string, 5)

	go func() {
		stream.control.Add(1)
		defer stream.control.Done()
		defer log.Debug("Scroll cleaner stopped")

		var scrolls []string

	receive:
		for {
			select {
			case scrollID, ok := <-discardID:
				if !ok {
					break receive
				}
				if scrollID == "" {
					continue
				}
				scrolls = append(scrolls, scrollID)
			}
		}
		log.Debugf("Clearing %d scrolls", len(scrolls))
		clear := l.Client.ClearScroll(scrolls...)
		_, err := clear.Do()
		if err != nil {
			log.Warnf("Error clearing scrolls.")
		}
	}()

	defer close(stream.Results)
	defer close(stream.Errors)
	defer close(discardID)

scrollLoop:
	for {
		if nextScrollID != "" {
			log.Debugf("Fetching next page using scrollID %s", nextScrollID[:10])
			scroll.ScrollId(nextScrollID)
			if resultCount >= spec.Size {
				break scrollLoop
			}
		} else {
			log.Debug("Fetching first page of scroll")
		}

		results, err := scroll.Do()
		if err != nil {
			log.Debugf("An error was returned during scroll after %d results.", resultCount)
			if err != elastic.EOS {
				stream.Errors <- errors.Annotate(err, "Server responded with error while scrolling.")
			}
			break scrollLoop
		}

		if results.ScrollId != "" {
			if results.ScrollId != nextScrollID {
				discardID <- nextScrollID
				nextScrollID = results.ScrollId
				log.Debugf("New scrollID returned: %s", nextScrollID[:10])
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
				resultCount++
			}
			if resultCount == spec.Size {
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
