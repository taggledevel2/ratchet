// Package ratchet contains primitives for managing high-level execution and
// data passing within a data pipeline.
package ratchet

import (
	"fmt"
	"strings"
)

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	Starter PipelineStarter
	Stages  []PipelineStage
}

// NewPipeline returns a a new *Pipeline given a starter and variable number of stages that will
// process data.
func NewPipeline(starter PipelineStarter, stages ...PipelineStage) *Pipeline {
	p := &Pipeline{Starter: starter, Stages: stages}
	return p
}

// Run should be called on a Pipeline object to kick things off. It will setup the
// data channels and connect them between each stage. Finally, Run() will be called
// on the PipelineStarter to let it know to start grabbing and sending data into the
// subsequent Pipeline
//
// Run will return an killChan that should be waited on so your main function doesn't
// return prematurely. Any stage of the pipeline can send to the killChan to halt
// execution. Your main() function check if the value is an error or nil to know if
// execution was a failure or a success (nil being the success value).
func (p *Pipeline) Run() (killChan chan error) {
	// Print an overview of the pipeline
	stageNames := []string{fmt.Sprintf("%v", p.Starter)}
	for _, s := range p.Stages {
		stageNames = append(stageNames, fmt.Sprintf("%v", s))
	}
	LogStatus("Pipeline: running [", strings.Join(stageNames, " -> "), "]")

	// Each stage should be connected by a separate channel object,
	// so that when close()'d it will cut off the processing in a left
	// to right manner.
	// Example:
	//	Pipeline: |Starter| dataChan0-> |Stage0| dataChan1-> |Stage1| -> killChan

	dataChans := make([]chan Data, len(p.Stages))
	for i := range dataChans {
		dataChans[i] = make(chan Data)
	}
	killChan = make(chan error)

	// Setup the channel and data-handling between all the Stages
	// (Refer to example above for understanding how indexes are mapped)
	for i, stage := range p.Stages {
		inputChan := dataChans[i]
		var outputChan chan Data
		if i < len(p.Stages)-1 {
			outputChan = dataChans[i+1]
		}
		// Each stage runs in it's own goroutine
		go processStage(stage, killChan, inputChan, outputChan)
	}

	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	go func() {
		outputChan := dataChans[0]
		LogDebug("Pipeline: Starting", p.Starter)
		p.Starter.Start(outputChan, killChan)
	}()

	return killChan
}

func processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan Data) {
	LogDebug("Pipeline: Stage", stage, "waiting for data on chan", inputChan)
	for data := range inputChan {
		LogDebug("Pipeline: Stage", stage, "receiving data:", data)
		stage.HandleData(data, outputChan, killChan)
	}
	LogDebug("Pipeline: Stage", stage, "finishing")
	stage.Finish(outputChan, killChan)
	LogInfo("Pipeline: Stage", stage, "finished")
	// If this is the final stage, exit the pipeline
	if outputChan == nil {
		LogStatus("Pipeline: Final stage (", stage, ") complete. Exiting pipeline.")
		killChan <- nil
	}
}
