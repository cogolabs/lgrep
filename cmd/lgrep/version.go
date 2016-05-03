package main

var (
	Version string
	Commit  string
)

func init() {
	if Version == "" {
		Version = "1.0.0-dev"
	}
	if Commit == "" {
		Commit = "HEAD"
	}
}
