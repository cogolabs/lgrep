package lgrep

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v3"
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
func (l LGrep) execute(search *elastic.SearchService, query elastic.Query, spec SearchOptions) (stream *SearchStream) {
	stream = &SearchStream{
		Results: make(chan Result, 93),
		Errors:  make(chan error, 1),
	}
	stream.control.quit = make(chan struct{}, 1)

	var (
		service Searcher = search
		scroll  *elastic.ScrollService
	)

	if spec.Size > 10000 {
		scroll = l.Scroll()
		spec.configureScroll(scroll)
		scroll.Query(query)
		service = scroll
	}

	go l.executeLoop(service, scroll, spec, stream)

	return stream
}

func (l LGrep) executeLoop(service Searcher, scroll *elastic.ScrollService, spec SearchOptions, stream *SearchStream) {
	// Start worker
	stream.control.Add(1)

	defer close(stream.Results)
	defer close(stream.Errors)

	var (
		scrolls []string
	)

searchLoop:
	for {
		result, err := service.Do()

		if err != nil {
			// End of the scroll
			if err == elastic.EOS {
				break
			}
			// Any other error
			stream.Errors <- err
			break
		}

		for i := range result.Hits.Hits {
			select {
			case <-stream.control.quit:
				break searchLoop
			default:
				result, err := extractResult(result.Hits.Hits[i], spec)
				if err != nil {
					stream.Errors <- err
				}
				stream.Results <- result
			}
		}

		// Gotta scroll!
		if scroll != nil {
			scrolls = append(scrolls, result.ScrollId)
			scroll.ScrollId(scrolls[len(scrolls)-1])
		} else {
			break searchLoop
		}
	}

	if len(scrolls) != 0 {
		log.Debugf("Cleaning up %d scrolls", len(scrolls))
		// Clean up used scrolls
		_, err := l.ClearScroll(scrolls...).Do()
		if err != nil {
			log.Debug("Error cleaning up scroll, they'll expire")
			stream.Errors <- err
		}
	}

	// Worker finished
	stream.control.Done()
}
