package ratchet

import (
	"fmt"
	"strings"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// StartSignal is what's sent to a starting PipelineStage.
var StartSignal = "GO"

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	Stages [][]PipelineStage
	timer  *util.Timer
	wg     sync.WaitGroup
}

// NewPipeline returns a new basic Pipeline given one or more stages that will
// process data.
func NewPipeline(stages ...PipelineStage) *Pipeline {
	p := &Pipeline{}
	for _, s := range stages {
		p.Stages = append(p.Stages, []PipelineStage{s})
	}
	return p
}

// NewBranchingPipeline creates a new Pipeline that supports branching
// and merging between stages, where each stage can have multiple
// PipelineStage instances running concurrently.
//
// Basic branching pipeline:
//                  /-> [stage1] \
//     [starter1] ----> [stage2] ---> [stage4]
//                  \-> [stage3] /
//
// More advanced branching pipeline:
//         (A)           (B)          (C)              (D)
//     [starter1] \             /-> [stage2] \   /-> [stage5]
//     [starter2] ---> [stage1] --> [stage3] ----
//     [starter3] /             \-> [stage4] /   \-> [stage6]
// Here is a step-by-step of what's happening in this more advanced example:
//
// (A) Three starters run concurrently, each sending data on their outputChan.
//
// (B) Stage1 receives separate data payloads from all three starters.
//
// (C) When stage1 sends data on it's outputChan, all of stages2-4 receive a copy of the same data payload.
//
// (D) This last stage works similarly as the prior steps. stages5&6 both receive a copy of data sent from all of stages2-4.
//
func NewBranchingPipeline(stageSlices ...[]PipelineStage) *Pipeline {
	p := &Pipeline{Stages: stageSlices}
	return p
}

// Run should be called on a Pipeline object to kick things off. It will setup the
// data channels and connect them between each stage. After calling Run(), the
// starting PipelineStage instances will receive a single HandleData function call where
// the data payload is StartSignal.
//
// Run will return a killChan that should be waited on so your main function doesn't
// return prematurely. Any stage of the pipeline can send to the killChan to halt
// execution. Your main() function should check if the sent value is an error or nil to know if
// execution was a failure or a success (nil being the success value).
func (p *Pipeline) Run() (killChan chan error) {
	p.timer = util.StartTimer()

	// Print an overview of the pipeline
	stageNames := []string{}
	for _, s := range p.Stages {
		stageNames = append(stageNames, fmt.Sprintf("%v", s))
	}
	logger.Status("Pipeline: running", strings.Join(stageNames, " -> "))

	// Each stage should be connected by a separate channel object,
	// so that when close()'d it will cut off the processing in a left
	// to right manner.
	// Example:
	//	Pipeline: |StarterStage| dataChan0-> |Stage0| dataChan1-> |Stage1| -> killChan

	dataChans := make([]chan data.JSON, len(p.Stages)-1)
	for i := range dataChans {
		dataChans[i] = make(chan data.JSON)
	}
	logger.Debug("Pipeline: data channels", dataChans)
	killChan = make(chan error)

	p.setupStages(killChan, dataChans)

	go func() {
		p.wg.Wait()
		logger.Status("Pipeline: Exiting pipeline.")
		logger.Status("Pipeline:", p.timer.Stop())
		killChan <- nil
	}()

	return killChan
}

func (p *Pipeline) setupStages(killChan chan error, dataChans []chan data.JSON) {
	// Setup the channel and data-handling between all the Stages except the
	// starting ones (which will be setup a bit differently afterwards).
	// (Refer to example above for understanding how indexes are mapped)
	nonStartingStages := p.Stages[1:]
	for i, stages := range nonStartingStages {
		inputChan := dataChans[i]
		var outputChan chan data.JSON
		if i < len(nonStartingStages)-1 {
			outputChan = dataChans[i+1]
		}

		logger.Debug("Pipeline: ", stages, "has inputChan", inputChan, "/ outputChan", outputChan)

		// Check if we're setting up a branching vs non-branching stage
		if len(stages) > 1 {
			// Setup branching and merging
			inputs := branchChans(inputChan, len(stages))
			// no merging need for final stage
			if outputChan != nil {
				outputs := make([]chan data.JSON, len(stages))
				for i := range outputs {
					outputs[i] = make(chan data.JSON)
				}
				mergeChans(outputs, outputChan)
				for i, s := range stages {
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[i], outputs[i])
					p.wg.Add(1)
				}
			} else {
				for i, s := range stages {
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[i], nil)
					p.wg.Add(1)
				}
			}
		} else {
			// If there's just a single Stage to run, no need to set up
			// extra channels for branching/merging.
			go p.processStage(stages[0], killChan, inputChan, outputChan)
			p.wg.Add(1)
		}
	}

	p.setupStartingStages(killChan, dataChans[0])
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
	p.wg.Done()
}

func (p *Pipeline) setupStartingStages(killChan chan error, outputChan chan data.JSON) {
	startingStages := p.Stages[0]
	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	if len(startingStages) > 1 {
		outputs := make([]chan data.JSON, len(startingStages))
		for i := range outputs {
			outputs[i] = make(chan data.JSON)
		}
		mergeChans(outputs, outputChan)
		for i, s := range startingStages {
			// Each stage runs in it's own goroutine
			go p.processStartingStage(s, killChan, outputs[i])
			p.wg.Add(1)
		}
	} else {
		// In the single-starter case, no need to create extra channels
		// for branching/merging.
		go p.processStartingStage(startingStages[0], killChan, outputChan)
		p.wg.Add(1)
	}
}

func (p *Pipeline) processStartingStage(startingStage PipelineStage, killChan chan error, outputChan chan data.JSON) {
	logger.Debug("Pipeline: Starting", startingStage)
	startingStage.HandleData([]byte(StartSignal), outputChan, killChan)
	startingStage.Finish(outputChan, killChan)
	p.wg.Done()
}

// Reference: http://blog.golang.org/pipelines
func mergeChans(inputs []chan data.JSON, output chan data.JSON) {
	logger.Debug("pipeline: merging channels", inputs, "into channel", output)

	var wg sync.WaitGroup

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	mergeData := func(c chan data.JSON) {
		for n := range c {
			output <- n
		}
		wg.Done()
	}
	wg.Add(len(inputs))
	for _, c := range inputs {
		go mergeData(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(output)
	}()
}

func branchChans(input chan data.JSON, numCopies int) []chan data.JSON {
	outputs := make([]chan data.JSON, numCopies)
	for i := range outputs {
		outputs[i] = make(chan data.JSON)
	}

	logger.Debug("pipeline: branching channel", input, "into channels", outputs)

	// Receive all data from input, sending the data receieved
	// on to all the outputs
	go func() {
		for d := range input {
			for _, out := range outputs {
				out <- d
			}
		}
		// Once all data is received from input, also close all the outputs
		for _, out := range outputs {
			close(out)
		}
	}()

	return outputs
}
