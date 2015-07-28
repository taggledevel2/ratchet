package stages

import (
	"regexp"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// RegexpMatcher checks if incoming data matches the given Regexp, and sends
// it on to the output channel only if it matches.
// It is using regexp.Match under the covers: https://golang.org/pkg/regexp/#Match
type RegexpMatcher struct {
	pattern string
}

// NewRegexpMatcher returns a new RegexpMatcher initialized
// with the given pattern to match.
func NewRegexpMatcher(pattern string) *RegexpMatcher {
	return &RegexpMatcher{pattern}
}

// ProcessData - see interface for documentation.
func (r *RegexpMatcher) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	matches, err := regexp.Match(r.pattern, d)
	util.KillPipelineIfErr(err, killChan)
	if matches {
		outputChan <- d
	}
}

// Finish - see interface for documentation.
func (r *RegexpMatcher) Finish(outputChan chan data.JSON, killChan chan error) {
	close(outputChan)
}

func (r *RegexpMatcher) String() string {
	return "RegexpMatcher"
}
