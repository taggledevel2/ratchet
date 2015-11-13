package processors

import (
	"fmt"
	"os/exec"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

// SCP executes the scp command, sending the given file to the given destination.
type SCP struct {
	Port        string
	Object      string
	Destination string
	// command     *exec.Cmd
}

func NewSCP(obj string, destination string) *SCP {
	return &SCP{Object: obj, Destination: destination}
}

func (s *SCP) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}

func (s *SCP) Finish(outputChan chan data.JSON, killChan chan error) {
	s.Run(killChan)
}

func (s *SCP) Run(killChan chan error) {
	scpParams := []string{}
	if s.Port != "" {
		scpParams = append(scpParams, fmt.Sprintf("-P %v", s.Port))
	}
	scpParams = append(scpParams, s.Object)
	scpParams = append(scpParams, s.Destination)

	cmd := exec.Command("scp", scpParams...)
	_, err := cmd.Output()
	util.KillPipelineIfErr(err, killChan)
}
