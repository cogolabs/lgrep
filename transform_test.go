package lgrep

import (
	"fmt"
	"testing"
	"time"
)

const (
	TestTS = "2016-04-29T13:58:59.420Z"
)

var (
	TestTSTime time.Time
)

func init() {
	TestTSTime, _ = time.Parse(time.RFC3339, TestTS)
}

func TestTranformTimestamp(t *testing.T) {
	fmt.Println(TestTSTime)
	t.Fail()
}
