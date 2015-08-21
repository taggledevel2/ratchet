package stages

import (
	"fmt"
	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
	"os/exec"
)

type Scp struct {
	Port        string
	Object      string
	Destination string
	// command     *exec.Cmd
}

func NewScp(obj string, destination string) *Scp {
	return &Scp{Object: obj, Destination: destination}
}

func (s *Scp) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	outputChan <- d
}

func (s *Scp) Finish(outputChan chan data.JSON, killChan chan error) {
	s.Run(killChan)
	if outputChan != nil {
		close(outputChan)
	}
}

func (s *Scp) Run(killChan chan error) {
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
