package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/codegangsta/cli"
	"github.com/juju/errors"
)

type githubRelease struct {
	// ID is the github release id
	ID int
	// Tag is the tag name that's associated with the release
	Tag string `json:"tag_name"`
	// Prerelease is a non-production use release
	Prerelease bool
	// URL is the human link
	URL string `json:"html_url"`

	semver
}

type semver struct {
	VersionMajor int64
	VersionMinor int64
	VersionRev   int64
}

func (s semver) NewerThan(ver semver) bool {
	if s.VersionMajor > ver.VersionMajor {
		return true
	}
	if s.VersionMinor > ver.VersionMinor {
		return true
	}
	if s.VersionRev > ver.VersionRev {
		return true
	}

	return false
}

func checkReleases(url string) (rel *githubRelease, err error) {
	c := http.Client{Timeout: time.Millisecond * 200}
	res, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	releases := make([]githubRelease, 0)
	err = json.Unmarshal(data, &releases)
	if err != nil {
		return nil, err
	}
	if len(releases) == 0 {
		return nil, nil
	}
	rel = &releases[0]
	rel.semver, err = parseVersion(rel.Tag)
	if err != nil {
		return nil, err
	}
	return rel, nil
}

func checkForUpdates(myVersion string) (update *githubRelease, err error) {
	newRel, err := checkReleases("https://api.github.com/repos/logstash/lgrep/releases")
	if err != nil {
		return nil, err
	}
	if newRel == nil {
		return nil, err
	}
	prog, err := parseVersion(myVersion)
	if newRel.NewerThan(prog) {
		return newRel, nil
	}

	return nil, nil
}

// RunCheckUpdate checks for any updates
func RunCheckUpdateOnError(c *cli.Context, cmdErr error, isSubcommand bool) (err error) {
	fmt.Println("An error occurred with your usage, check --help")

	update, _ := checkForUpdates(Version)
	if update != nil {
		fmt.Printf("\nThere's an update available! See %s\n", update.URL)
	}
	return nil
}

func parseVersion(ver string) (v semver, err error) {
	r := regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)
	matches := r.FindStringSubmatch(ver)
	if len(matches) == 0 {
		return v, errors.Errorf("Cannot parse version from '%s'", ver)
	}
	major, _ := strconv.ParseInt(matches[1], 10, 64)
	minor, _ := strconv.ParseInt(matches[2], 10, 64)
	rev, _ := strconv.ParseInt(matches[3], 10, 64)

	return semver{major, minor, rev}, nil

}
