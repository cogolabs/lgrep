package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cogolabs/lgrep"
)

var (
	flagCount    = flag.Int("n", 100, "Number of results to fetch")
	flagOffset   = flag.Int("o", 0, "Offset of fetch, use to roll through many results")
	flagFields   = flag.String("f", "", "Fields to return, causes results to be rendered as json")
	flagEndpoint = flag.String("e", "http://localhost:9200/", "Elasticsearch endpoint")
	flagIndex    = flag.String("i", "*-*", "Index to search")
)

func init() {
	log.SetOutput(os.Stderr)
}

func main() {
	query := strings.Join(os.Args, " ")
	lg, err := lgrep.NewLGrep(*flagEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	healthy, err := lg.Health()
	if !healthy || err != nil {
		log.Fatal("Elasticsearch cluster is not healthy or available")
	}
	results, err := lg.Search(query, *flagIndex, *flagCount, *flagOffset)
	if err != nil {
		log.Fatal(err)
	}
	msgs, err := lg.Messages(results)
	if err != nil {
		if len(msgs) != 0 {
			log.Println(err)
		} else {
			log.Fatal(err)
		}
	}
	for _, m := range msgs {
		fmt.Println(m)
	}
}
