package lgrep

import (
	"context"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"gopkg.in/olivere/elastic.v3"
)

const (
	// MaxSearchSize is the maximum search size that is able to be
	// performed before the search will necessitate a scroll.
	MaxSearchSize   = 10000
	scrollChunk     = 100
	scrollKeepalive = "30s"
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
		Results: make(chan Result, scrollChunk),
		Errors:  make(chan error, 1),
	}
	if spec.QueryDebug {
		log.SetLevel(log.DebugLevel)
	}
	stream.control.quit = make(chan struct{}, 1)
	stream.control.WaitGroup = &sync.WaitGroup{}

	if spec.Size > MaxSearchSize {
		log.Debugf("searching with scroll for large size (%d)", spec.Size)

		if spec.Index == "" || (spec.Index == "" && len(spec.Indices) == 0) {
			return nil, errors.New("An index pattern must be given for large requests")
		}

		source, err := query.Source()
		if err != nil {
			return nil, err
		}

		scroll := l.Scroll()
		scroll.KeepAlive(scrollKeepalive)
		spec.configureScroll(scroll)
		// reset to the chunk size, otherwise the entire result will
		// (attempt to) be pulled in a single request
		scroll.Size(scrollChunk)

		if queryMap, ok := source.(map[string]interface{}); ok {
			log.Debugf("QueryMap provided, merging with specifications")
			qm := QueryMap(queryMap)
			spec.configureQueryMap(qm)
			qm["size"] = scrollChunk
			log.Debugf("QueryMap result: %#v", qm)
			scroll.Body(qm)
		} else {
			// TODO: Verify any other query type and pass it into the query for the user.
			log.Errorf("cannot execute scroll with provided query, unhandled")
			return nil, errors.New("cannot execute scroll with provided query, unhandled")
		}

		go l.executeScroll(scroll, query, spec, stream)
	} else {
		log.Debugf("searching with regular query for small size (%d)", spec.Size)
		go l.executeSearcher(search, query, spec, stream)
	}

	return stream, nil
}

func (l LGrep) executeScroll(scroll *elastic.ScrollService, query elastic.Query, spec SearchOptions, stream *SearchStream) {
	stream.control.Add(1)
	defer stream.control.Done()

	var (
		resultCount  int
		nextScrollID string
	)

	defer close(stream.Results)
	defer close(stream.Errors)

	ctx, cancelReq := context.WithCancel(context.TODO())

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

		results, err := scroll.DoC(ctx)
		if err != nil {
			log.Debugf("An error was returned during scroll after %d results.", resultCount)
			if err != elastic.EOS {
				stream.Errors <- errors.Annotate(err, "Server responded with error while scrolling.")
			}
			break scrollLoop
		}

		if results.ScrollId != "" {
			if results.ScrollId != nextScrollID {
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
				cancelReq()
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

	l.ClearScroll(nextScrollID).Do()
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
			doc, err := extractResult(result.Hits.Hits[i], spec)
			if err != nil {
				stream.Errors <- err
				continue
			}
			stream.Results <- doc
		}
	}
}
