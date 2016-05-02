package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/cogolabs/lgrep"
	"github.com/juju/errors"
)

const (
	// DefaultFormat provides a sane default to use in the case that the
	// user does not provide a format.
	DefaultFormat = ".message"
	// StdlineFormat provides a common usable format
	StdlineFormat = ".host .service .message"
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

var (
	// GlobalFlags apply to the entire application
	GlobalFlags = []cli.Flag{
		cli.StringFlag{
			Name:   "endpoint, e",
			Value:  "http://localhost:9200/",
			Usage:  "Elasticsearch Endpoint",
			EnvVar: "LGREP_ENDPOINT",
		},

		cli.BoolFlag{
			Name:  "debug, D",
			Usage: "Debug lgrep run with verbose logging",
		},
	}

	// QueryFlags apply to runs that query with lgrep
	QueryFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "size, n",
			Usage: "Number of results to be returned",
			Value: 100,
		},
		cli.StringFlag{
			Name:  "format, f",
			Usage: "Simple formatting using text/template (go stdlib)",
			Value: DefaultFormat,
		},
		cli.BoolFlag{
			Name:  "stdline, ff",
			Usage: "Format lines with common format '" + StdlineFormat + "'.",
		},
		cli.BoolFlag{
			Name:  "tabulate, T",
			Usage: "Tabulate the data into columns",
		},
		cli.BoolFlag{
			Name:   "query-debug, QD",
			Usage:  "Log query sent to the server",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "query-index, Qi",
			Usage: "Query this index in elasticsearch, if not provided - all indicies",
		},
		cli.StringFlag{
			Name:  "query-fields, Qc",
			Usage: "Fields to be retrieved (ex: field1,field2)",
		},
	}
)

func App() *cli.App {
	app := cli.NewApp()
	app.Name = "lgrep"
	app.Version = "1.0.0"
	app.EnableBashCompletion = true

	// Set up the application based on flags before handing off to the action
	app.Before = RunPrepareApp
	app.Action = RunQuery
	app.UsageText = "lgrep [options] QUERY"
	app.After = func(c *cli.Context) error {
		for _, f := range c.GlobalFlagNames() {
			fmt.Printf("%s = %s\n", f, c.Generic(f))
		}
		return nil
	}
	app.Flags = append(app.Flags, GlobalFlags...)
	app.Flags = append(app.Flags, QueryFlags...)
	app.Usage = `

Reference time: Mon Jan 2 15:04:05 -0700 MST 2006

Text formatting
given: { "timestamp": "2016-04-29T13:58:59.420Z" }

{{.timestamp|ftime "15:04"}} => 13:58
{{.timestamp|ftime "2006-01-02 15:04"}} => 2016-04-29 13:58
`
	return app
}

// RunPrepareApp sets defaults and verifies the arguments and flags
// passed to the application.
func RunPrepareApp(c *cli.Context) (err error) {
	// query might have been provided via a file or another flag
	var queryProvided bool

	// Set the format to the stdline format if asked, and warn when
	// they're both set.
	if c.Bool("stdline") {
		if c.IsSet("format") {
			log.Warn("You've provided a format (-f) and asked for the stdline format (-ff), using stdline!")
		}
		c.Set("format", StdlineFormat)
	}

	if c.Bool("debug") {
		log.SetLevel(log.DebugLevel)
	}

	if len(c.Args()) == 0 && !queryProvided {
		return cli.NewExitError("No query provided", 3)
	}

	return err
}

func RunQuery(c *cli.Context) (err error) {
	return err
}

func init() {
	// flag.Usage = usage
	// flag.Parse()
	// log.SetOutput(os.Stderr)
	// log.SetLevel(log.WarnLevel)

	// if *flagDebug {
	// 	log.SetLevel(log.DebugLevel)
	// 	*flagQueryDebug = true
	// }
}

func main() {
	App().Run(os.Args)
	return
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
		format = StdlineFormat
	}

	if *flagResultTabulate {
		if format == DefaultFormat || format == StdlineFormat {
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
	if len(msgs) == 0 {
		log.Warn("Query returned zero results")
		os.Exit(1)
	}
	for _, m := range msgs {
		fmt.Fprintln(out, m)
	}
}
