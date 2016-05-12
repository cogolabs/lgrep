package main

var (
	// Version is the current tagged release or relative release
	// identifier.
	Version string
	// Commit is the short SHA of the last commit that was made when the
	// tool was built.
	Commit string
)

func init() {
	if Version == "" {
		Version = "1.3.1-dev"
	}
	if Commit == "" {
		Commit = "HEAD"
	}
}
