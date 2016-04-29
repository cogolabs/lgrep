package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cogolabs/lgrep"
	log "github.com/Sirupsen/logrus"
)

var (
	flagEndpoint = flag.String("e", "http://localhost:9200/", "Elasticsearch endpoint")

	flagQueryIndex = flag.String("i", "", "Index to search")
	flagQueryDebug = flag.Bool("QD", false, "Debug the query sent to elasticsearch")
	flagQuerySort  = flag.String("Qs", "@timestamp:desc", "Sort returned data by <field>:<asc|desc>")

	flagResultCount  = flag.Int("n", 100, "Number of results to fetch")
	flagResultFormat = flag.String("f", "{{.message}}", "Format returned results into text/template format")
	flagResultFields = flag.String("c", "", "Fields to return, causes results to be rendered as json")
)

func init() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}

func main() {
	q := strings.Join(flag.Args(), " ")
	lg, err := lgrep.NewLGrep(*flagEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	lg.Debug = *flagQueryDebug
	docs, err := lg.SimpleSearch(q, *flagResultCount)
	if err != nil {
		log.Fatal(err)
	}
	msgs, err := lg.FormatSources(docs, *flagResultFormat)
	for _, m := range msgs {
		fmt.Println(m)
	}
}
