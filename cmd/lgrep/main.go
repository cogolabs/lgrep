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
	// DefaultFormat provides a sane default to use in the case that the
	// user does not provide a format.
	DefaultFormat = ".message"
	// DefaultVerboseFormat provides a more verbose default format.
	DefaultVerboseFormat = ".host .service .message"
)

var (
	flagEndpoint = flag.String("e", "http://localhost:9200/", "Elasticsearch endpoint")
	flagDebug    = flag.Bool("D", false, "Debug lgrep")

	flagQueryIndex = flag.String("Qi", "", "Index to search")
	flagQueryDebug = flag.Bool("QD", false, "Debug the query sent to elasticsearch")
	//flagQuerySort  = flag.String("Qs", "timestamp:desc", "Sort by <field>:<asc|desc> (appended when specified)")
	//flagQueryRegex = flag.String("Qr", "message:^.*$", "Add a regex query to the search (AND'd)")

	flagResultCount         = flag.Int("n", 100, "Number of results to fetch")
	flagResultFormat        = flag.String("f", DefaultFormat, "Format returned results into text/template format")
	flagResultVerboseFormat = flag.Bool("vf", false, "Use a default verbose format")
	flagResultFields        = flag.String("c", "", "Fields to return, causes results to be rendered as json")
	flagResultTabulate      = flag.Bool("T", false, "Write out as tabulated data")
)

func usage() {
	fmt.Fprint(os.Stderr, "lgrep - Logstash grep\n\n")
	flag.PrintDefaults()

	fmt.Fprint(os.Stderr, `

Reference time: Mon Jan 2 15:04:05 -0700 MST 2006\n

given: { "timestamp": "2016-04-29T13:58:59.420Z" }

{{.timestamp|ftime "15:04"}} => 13:58
{{.timestamp|ftime "2006-01-02 15:04"}} => 2016-04-29 13:58
`)
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
	lg, err := lgrep.New(*flagEndpoint)
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

	msgs, err := lgrep.FormatSources(docs, format)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Fprintln(out, m)
	}
}
