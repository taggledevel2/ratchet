// Package ratchet contains primitives for managing high-level execution and
// data passing within a data pipeline.
package ratchet

import (
	"fmt"
	"strings"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	Starter PipelineStarter
	Stages  []PipelineStage
	timer   *util.Timer
}

// NewPipeline returns a a new *Pipeline given a starter and one or more stages that will
// process data.
func NewPipeline(starter PipelineStarter, stages ...PipelineStage) *Pipeline {
	p := &Pipeline{Starter: starter, Stages: stages}
	return p
}

// Run should be called on a Pipeline object to kick things off. It will setup the
// data channels and connect them between each stage. Finally, Start() will be called
// on the PipelineStarter to let it know to start grabbing and sending data into the
// subsequent PipelineStages.
//
// Run will return a killChan that should be waited on so your main function doesn't
// return prematurely. Any stage of the pipeline can send to the killChan to halt
// execution. Your main() function should check if the sent value is an error or nil to know if
// execution was a failure or a success (nil being the success value).
func (p *Pipeline) Run() (killChan chan error) {
	// Print an overview of the pipeline
	stageNames := []string{fmt.Sprintf("%v", p.Starter)}
	for _, s := range p.Stages {
		stageNames = append(stageNames, fmt.Sprintf("%v", s))
	}
	logger.Status("Pipeline: running [", strings.Join(stageNames, " -> "), "]")

	// Each stage should be connected by a separate channel object,
	// so that when close()'d it will cut off the processing in a left
	// to right manner.
	// Example:
	//	Pipeline: |Starter| dataChan0-> |Stage0| dataChan1-> |Stage1| -> killChan

	dataChans := make([]chan data.JSON, len(p.Stages))
	for i := range dataChans {
		dataChans[i] = make(chan data.JSON)
	}
	killChan = make(chan error)

	p.timer = util.StartTimer()

	// Setup the channel and data-handling between all the Stages
	// (Refer to example above for understanding how indexes are mapped)
	for i, stage := range p.Stages {
		inputChan := dataChans[i]
		var outputChan chan data.JSON
		if i < len(p.Stages)-1 {
			outputChan = dataChans[i+1]
		}
		// Each stage runs in it's own goroutine
		go p.processStage(stage, killChan, inputChan, outputChan)
	}

	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	go func() {
		outputChan := dataChans[0]
		logger.Debug("Pipeline: Starting", p.Starter)
		p.Starter.Start(outputChan, killChan)
	}()

	return killChan
}

func (p *Pipeline) processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan data.JSON) {
	logger.Debug("Pipeline: Stage", stage, "waiting for data on chan", inputChan)
	for d := range inputChan {
		logger.Debug("Pipeline: Stage", stage, "receiving data:", string(d))
		stage.HandleData(d, outputChan, killChan)
	}
	logger.Debug("Pipeline: Stage", stage, "finishing...")
	stage.Finish(outputChan, killChan)
	logger.Info("Pipeline: Stage", stage, "finished.", p.timer)
	// If this is the final stage, exit the pipeline
	if outputChan == nil {
		logger.Status("Pipeline: Final stage (", stage, ") complete. Exiting pipeline.")
		logger.Status("Pipeline:", p.timer.Stop())
		killChan <- nil
	}
}
