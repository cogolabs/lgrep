package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cogolabs/lgrep"
)

var (
	flagCount      = flag.Int("n", 100, "Number of results to fetch")
	flagOffset     = flag.Int("o", 0, "Offset of fetch, use to roll through many results")
	flagFields     = flag.String("f", "", "Fields to return, causes results to be rendered as json")
	flagEndpoint   = flag.String("e", "http://localhost:9200/", "Elasticsearch endpoint")
	flagIndex      = flag.String("i", "", "Index to search")
	flagQueryDebug = flag.Bool("QQ", false, "Debug the query sent to elasticsearch")
)

func init() {
	flag.Parse()
	log.SetOutput(os.Stderr)
}

func main() {
	query := strings.Join(flag.Args(), " ")
	lg, err := lgrep.NewLGrep(*flagEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	//results, err := lg.Search(query, *flagIndex, *flagCount, *flagOffset)
	results := make([]*json.RawMessage, 0)
	q, _ := lgrep.LuceneSearchQuery(query, *flagCount)
	if *flagQueryDebug {
		if d, err := json.MarshalIndent(q, "", "  "); err == nil {
			fmt.Printf("%s\n", d)
			return
		}
	}

	result, err := lg.Client.Search(*flagIndex).Source(q).Do()
	if err != nil {
		log.Fatal(err)
	}
	for _, source := range result.Hits.Hits {
		results = append(results, source.Source)
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
