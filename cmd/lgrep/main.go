package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
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
		cli.BoolFlag{
			Name:  "check-for-updates, U",
			Usage: "Check github for a new release",
		},
	}

	// QueryFlags apply to runs that query with lgrep
	QueryFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "format, f",
			Usage: "Simple formatting using text/template (go stdlib)",
			Value: DefaultFormat,
		},
		cli.BoolFlag{
			Name:  "raw-json, j",
			Usage: "Output the raw json _source of the results (1 line per result)",
		},
		cli.BoolFlag{
			Name:  "stdline, ff",
			Usage: "Format lines with common format '" + StdlineFormat + "'.",
		},
		cli.BoolFlag{
			Name:  "tabulate, T",
			Usage: "Tabulate the data into columns",
		},
		cli.IntFlag{
			Name:  "query-size, n, Qn",
			Usage: "Number of results to be returned",
			Value: 100,
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
		cli.StringFlag{
			Name:  "query-file, Qf",
			Usage: "Raw elasticsearch json query to submit",
		},
	}
)

// App instaniates the lgrep command line application for running.
func App() *cli.App {
	app := cli.NewApp()
	app.Name = "lgrep"
	app.Version = fmt.Sprintf("%s (%s)", Version, Commit)
	app.EnableBashCompletion = true

	// Set up the application based on flags before handing off to the action
	app.Before = RunPrepareApp
	app.Action = RunQuery
	app.OnUsageError = RunCheckUpdateOnError
	app.UsageText = "lgrep [options] QUERY"
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

func dumpFlags(c *cli.Context) (err error) {
	for _, f := range c.GlobalFlagNames() {
		fmt.Printf("%s = %s\n", f, c.Generic(f))
	}
	return nil
}

// RunPrepareApp sets defaults and verifies the arguments and flags
// passed to the application.
func RunPrepareApp(c *cli.Context) (err error) {
	// query might have been provided via a file or another flag
	var queryProvided bool

	if c.Bool("check-for-updates") {
		update, err := checkForUpdates(Version)
		if err != nil {
			return cli.NewExitError("Error during release check, check yourself please", 2)
		}
		if update != nil {
			fmt.Printf("There's an update available! See %s\n", update.URL)
		} else {
			fmt.Println("lgrep is up to date")
		}
		os.Exit(0)
	}

	if endpoint := c.String("endpoint"); endpoint == "" {
		return cli.NewExitError("Endpoint must be set", 1)
	} else if _, err := url.Parse(endpoint); err != nil {
		return cli.NewExitError("Endpoint must be a url (ex: http://localhost:9200/)", 1)
	}

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
		c.Set("query-debug", "true")
		dumpFlags(c)
	}

	if c.IsSet("query-file") {
		if _, err := os.Stat(c.String("query-file")); err != nil {
			return cli.NewExitError("Query file provided cannot be read", 3)
		}
		queryProvided = true
	}

	// Can't provide both a query via a file and via lucene search via
	// args.
	if len(c.Args()) > 0 && queryProvided {
		return cli.NewExitError("You've provided multiple queries (file and lucene perhaps?)", 3)
	}
	if len(c.Args()) == 0 && !queryProvided {
		return cli.NewExitError("No query provided", 3)
	}

	return err
}

// Config represents the configuration for the lgrep run based on the
// flags provided.
type Config struct {
	// General client configuration
	endpoint string
	debug    bool

	// Query configuration
	queryFile   string
	querySize   int
	queryIndex  string
	queryDebug  bool
	queryFields []string
	query       string

	// Formatting configuration
	formatTemplate string
	formatRaw      bool
	formatTabulate bool
}

// Run the user's configured search
func (c Config) search() (results []*json.RawMessage, err error) {
	l, err := lgrep.New(c.endpoint)
	if err != nil {
		log.Error(err)
		return results, err
	}

	l.Debug = c.queryDebug

	if c.queryFile != "" {
		var (
			f *os.File
			d []byte
		)
		f, err = os.Open(c.queryFile)
		if err != nil {
			return results, errors.Annotate(err, "Could not open the provided query file")
		}
		d, err = ioutil.ReadAll(f)
		if err != nil {
			return results, errors.Annotate(err, "Could not read the provided query file")
		}
		results, err = l.SearchWithSource(d)
	}

	if c.query != "" {
		results, err = l.SimpleSearch(c.query, c.queryIndex, c.querySize)
	}
	fmt.Println(len(results))

	return results, err
}

// Format and print the results according to config to the specified
// out.
func (c Config) format(results []*json.RawMessage, out io.Writer) error {
	if c.formatRaw {
		if len(c.queryFields) != 0 {
			log.Error("Field selection and raw output is unsupported at this time")
			return nil
		}
		for i := range results {
			fmt.Printf("%s\n", *results[i])
		}
		return nil
	}

	var (
		tabbed *tabwriter.Writer
		format = c.formatTemplate
	)
	if c.formatTabulate {
		format = tabifyFormat(c.formatTemplate, false)
		header := tabifyFormat(format, true)
		tabbed = tabwriter.NewWriter(out, 6, 2, 2, ' ', 0)
		out = tabbed
		defer tabbed.Flush()
		fmt.Fprintln(tabbed, header)
	}
	msgs, err := lgrep.Format(results, c.formatTemplate)
	if err != nil {
		return err
	}

	for i := range msgs {
		fmt.Fprintln(out, msgs[i])
	}
	return nil
}

// RunQuery is the primary action that the lgrep application performs.
func RunQuery(c *cli.Context) (err error) {
	run := Config{
		endpoint: c.String("endpoint"),
		debug:    c.Bool("debug"),

		queryFile:   c.String("query-file"),
		querySize:   c.Int("query-size"),
		queryIndex:  c.String("query-index"),
		queryDebug:  c.Bool("query-debug"),
		queryFields: []string{},
		query:       strings.Join(c.Args(), " "),

		formatTemplate: c.String("format"),
		formatRaw:      c.Bool("raw-json"),
		formatTabulate: c.Bool("tabulate"),
	}

	if qf := c.String("query-fields"); qf != "" {
		run.queryFields = strings.Split(qf, ",")
	}

	results, err := run.search()
	if err != nil {
		return err
	}

	if len(results) == 0 {
		log.Warn("0 results returned")
		return nil
	}
	return run.format(results, os.Stdout)
}

// tabifyFormat crafts a tabular format from a format string.
func tabifyFormat(format string, stripTokens bool) (str string) {
	// Format first for consistency in replacements
	format = lgrep.CurlyFormat(format)

	// Turn any number of spaces into tabs.
	spacerTab := regexp.MustCompile(`\s+`)
	str = spacerTab.ReplaceAllString(format, "\t")
	str = strings.TrimSpace(str)

	if !stripTokens {
		return str
	}

	// Remove tokens
	tokenRemove := regexp.MustCompile(`({{\.?|}})`)
	str = tokenRemove.ReplaceAllString(str, "")
	return str
}

func main() {
	App().Run(os.Args)
}
