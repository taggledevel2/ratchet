package ratchet

import (
	"fmt"
	"strings"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

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
type BranchingPipeline struct {
	Starters []PipelineStarter
	Stages   [][]PipelineStage
	timer    *util.Timer
	wg       sync.WaitGroup
}

func NewBranchingPipeline(starters []PipelineStarter, stages ...[]PipelineStage) *BranchingPipeline {
	p := &BranchingPipeline{Starters: starters, Stages: stages}
	return p
}

func (p *BranchingPipeline) Run() (killChan chan error) {
	p.timer = util.StartTimer()

	// Print an overview of the pipeline
	stageNames := []string{fmt.Sprintf("%v", p.Starters)}
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

	p.setupStages(killChan, dataChans)
	p.setupStarters(killChan, dataChans[0])

	go func() {
		p.wg.Wait()
		logger.Status("Pipeline: Exiting pipeline.")
		logger.Status("Pipeline:", p.timer.Stop())
		killChan <- nil
	}()

	return killChan
}

func (p *BranchingPipeline) setupStages(killChan chan error, dataChans []chan data.JSON) {
	// Setup the channel and data-handling between all the Stages
	// (Refer to example above for understanding how indexes are mapped)
	for i, stages := range p.Stages {
		inputChan := dataChans[i]
		var outputChan chan data.JSON
		if i < len(p.Stages)-1 {
			outputChan = dataChans[i+1]
		}

		// Check if we're setting up a branching vs non-branching stage
		if len(stages) > 1 {
			// Setup branching and merging
			inputs := branch(inputChan, len(stages))
			// no merging need for final stage
			if outputChan != nil {
				outputs := make([]chan data.JSON, len(stages))
				for i := range outputs {
					outputs[i] = make(chan data.JSON)
				}
				merge(outputs, outputChan)
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
}

func (p *BranchingPipeline) processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan data.JSON) {
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

func (p *BranchingPipeline) setupStarters(killChan chan error, outputChan chan data.JSON) {
	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	if len(p.Starters) > 1 {
		outputs := make([]chan data.JSON, len(p.Starters))
		for i := range outputs {
			outputs[i] = make(chan data.JSON)
		}
		merge(outputs, outputChan)
		for i, s := range p.Starters {
			// Each stage runs in it's own goroutine
			go p.processStarter(s, killChan, outputs[i])
			p.wg.Add(1)
		}
	} else {
		// In the single-starter case, no need to create extra channels
		// for branching/merging.
		go p.processStarter(p.Starters[0], killChan, outputChan)
		p.wg.Add(1)
	}
}

func (p *BranchingPipeline) processStarter(starter PipelineStarter, killChan chan error, outputChan chan data.JSON) {
	logger.Debug("Pipeline: Starting", starter)
	starter.Start(outputChan, killChan)
	p.wg.Done()
}

// Reference: http://blog.golang.org/pipelines
func merge(inputs []chan data.JSON, output chan data.JSON) {
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

func branch(input chan data.JSON, numCopies int) []chan data.JSON {
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
