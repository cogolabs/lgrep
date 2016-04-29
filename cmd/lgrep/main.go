package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cogolabs/lgrep"
	log "github.com/Sirupsen/logrus"
)

const (
	DefaultFormat        = ".message"
	DefaultVerboseFormat = ".host .service .message"
	RawFormat            = "."
)

var (
	flagEndpoint = flag.String("e", "http://localhost:9200/", "Elasticsearch endpoint")
	flagDebug    = flag.Bool("D", false, "Debug lgrep")

	flagQueryIndex = flag.String("Qi", "", "Index to search")
	flagQueryDebug = flag.Bool("QD", false, "Debug the query sent to elasticsearch")
	flagQuerySort  = flag.String("Qs", "@timestamp:desc", "Sort by <field>:<asc|desc> (appended when specified)")
	flagQueryRegex = flag.String("Qr", "message:^.*$", "Add a regex query to the search (AND'd)")

	flagResultCount         = flag.Int("n", 100, "Number of results to fetch")
	flagResultFormat        = flag.String("f", DefaultFormat, "Format returned results into text/template format")
	flagResultVerboseFormat = flag.Bool("vf", false, "Use a default verbose format")
	flagResultFields        = flag.String("c", "", "Fields to return, causes results to be rendered as json")
	flagResultTabulate      = flag.Bool("T", false, "Write out as tabulated data")
)

func usage() {
	fmt.Fprint(os.Stderr, "lgrep - Logstash grep\n\n")
	flag.PrintDefaults()
}

func init() {
	flag.Usage = usage
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetLevel(log.ErrorLevel)

	if *flagDebug {
		log.SetLevel(log.DebugLevel)
		*flagQueryDebug = true
	}
}

func main() {
	var out io.Writer
	out = os.Stdout

	q := strings.Join(flag.Args(), " ")
	lg, err := lgrep.NewLGrep(*flagEndpoint)
	if err != nil {
		log.Fatal(err)
	}
	lg.Debug = *flagQueryDebug
	docs, err := lg.SimpleSearch(q, *flagQueryIndex, *flagResultCount)
	if err != nil {
		log.Fatal(err)
	}
	format := *flagResultFormat
	if *flagResultVerboseFormat && format == DefaultFormat {
		format = DefaultVerboseFormat
	}

	if *flagResultTabulate {
		if format == DefaultFormat || format == DefaultVerboseFormat {
			format = strings.Replace(format, " ", "\t", -1)
		}
		tw := tabwriter.NewWriter(out, 5, 2, 2, ' ', 0)
		defer func() { tw.Flush() }()
		out = tw
	}

	msgs, err := lg.FormatSources(docs, format)
	for _, m := range msgs {
		fmt.Fprintln(out, m)
	}
}
