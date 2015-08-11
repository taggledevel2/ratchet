package ratchet

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// StartSignal is what's sent to a starting PipelineStage's ProcessData.
// Typically this value will be ignored.
var StartSignal = "GO"

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	Name        string // Name is simply for display purpsoses in log output.
	Stages      [][]PipelineStage
	stats       [][]*executionStats
	RecordStats bool // RecordStats can be set to false to avoid creating extra channels and some minor overhead.
	timer       *util.Timer
	wg          sync.WaitGroup
}

// NewPipeline returns a new basic Pipeline given one or more stages that will
// process data.
func NewPipeline(stages ...PipelineStage) *Pipeline {
	stageSlices := [][]PipelineStage{}
	for _, s := range stages {
		stageSlices = append(stageSlices, []PipelineStage{s})
	}
	return NewBranchingPipeline(stageSlices...)
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
	p := &Pipeline{Name: "Pipeline", Stages: stageSlices, RecordStats: true}

	// setup stats to match the stages
	p.stats = make([][]*executionStats, len(stageSlices))
	for i, ss := range stageSlices {
		if p.stats[i] == nil {
			p.stats[i] = make([]*executionStats, len(ss))
		}
		for j := range ss {
			p.stats[i][j] = &executionStats{}
		}
	}

	return p
}

// Run should be called on a Pipeline object to kick things off. It will setup the
// data channels and connect them between each stage. After calling Run(), the
// starting PipelineStage instances will receive a single ProcessData function call where
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
	logger.Status(p.Name, ": running", strings.Join(stageNames, " -> "))

	// Each stage should be connected by a separate channel object,
	// so that when close()'d it will cut off the processing in a left
	// to right manner.
	// Example:
	//	Pipeline: |StarterStage| dataChan0-> |Stage0| dataChan1-> |Stage1| -> killChan

	dataChans := initDataChans(len(p.Stages) - 1)
	killChan = make(chan error)
	logger.Debug(p.Name, ": data channels", dataChans)
	handleInterrupt(killChan)

	p.setupStages(killChan, dataChans)

	go func() {
		p.wg.Wait()
		logger.Status(p.Name, ": Exiting pipeline.")
		logger.Status(p.Name, ":", p.timer.Stop())
		killChan <- nil
	}()

	return killChan
}

func (p *Pipeline) setupStages(killChan chan error, dataChans []chan data.JSON) {
	// Setup the channel and data-handling between all the Stages except the
	// starting ones (which will be setup a bit differently afterwards).
	// (Refer to example above for understanding how indexes are mapped)
	nonStartingStages := p.Stages[1:]
	nonStartingStats := p.stats[1:]
	for i, stages := range nonStartingStages {
		inputChan := dataChans[i]
		var outputChan chan data.JSON
		if i < len(nonStartingStages)-1 {
			outputChan = dataChans[i+1]
		}
		stats := nonStartingStats[i]

		logger.Debug(p.Name, ": ", stages, "has inputChan", inputChan, "/ outputChan", outputChan)

		// Check if we're setting up a branching vs non-branching stage
		if len(stages) > 1 {
			// Setup branching and merging
			inputs := p.branchChans(inputChan, len(stages))
			// no merging needed for final stage
			if outputChan != nil {
				outputs := initDataChans(len(stages))
				p.mergeChans(outputs, outputChan)
				for j, s := range stages {
					out := outputs[j]
					stat := stats[j]
					if p.RecordStats {
						out = p.interceptChan(outputs[j], func(d data.JSON) {
							stat.recordDataSent(d)
						})
					}
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[j], out, stat)
					p.wg.Add(1)
				}
			} else {
				for j, s := range stages {
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[j], nil, stats[j])
					p.wg.Add(1)
				}
			}
		} else {
			// If there's just a single Stage to run, no need to set up
			// extra channels for branching/merging.
			go p.processStage(stages[0], killChan, inputChan, outputChan, stats[0])
			p.wg.Add(1)
		}
	}

	p.setupStartingStages(killChan, dataChans[0])
}

func (p *Pipeline) processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan data.JSON, stat *executionStats) {
	logger.Debug(p.Name, ": Stage", stage, "waiting for data on chan", inputChan)

	for d := range inputChan {
		logger.Info("Pipeline: Stage", stage, "receiving data:", len(d), "bytes")
		logger.Debug(string(d))
		if p.RecordStats {
			stat.recordDataReceived(d)
			stat.recordExecution(func() {
				stage.ProcessData(d, outputChan, killChan)
			})
		} else {
			stage.ProcessData(d, outputChan, killChan)
		}
	}

	logger.Debug(p.Name, ": Stage", stage, "finishing...")
	if p.RecordStats {
		stat.recordExecution(func() {
			stage.Finish(outputChan, killChan)
		})
	} else {
		stage.Finish(outputChan, killChan)
	}
	logger.Info(p.Name, ": Stage", stage, "finished.", p.timer)
	p.wg.Done()
}

func (p *Pipeline) setupStartingStages(killChan chan error, outputChan chan data.JSON) {
	startingStages := p.Stages[0]
	stats := p.stats[0]
	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	if len(startingStages) > 1 {
		outputs := initDataChans(len(startingStages))
		p.mergeChans(outputs, outputChan)
		for i, s := range startingStages {
			out := outputs[i]
			stat := stats[i]
			if p.RecordStats {
				out = p.interceptChan(outputs[i], func(d data.JSON) {
					stat.recordDataSent(d)
				})
			}
			// Each stage runs in it's own goroutine
			go p.processStartingStage(s, killChan, out, stat)
			p.wg.Add(1)
		}
	} else {
		out := outputChan
		stat := stats[0]
		// In the single-starter case, no need to create extra channels
		// for branching/merging.
		if p.RecordStats {
			out = p.interceptChan(outputChan, func(d data.JSON) {
				stat.recordDataSent(d)
			})
		}
		go p.processStartingStage(startingStages[0], killChan, out, stat)
		p.wg.Add(1)
	}
}

func (p *Pipeline) processStartingStage(startingStage PipelineStage, killChan chan error, outputChan chan data.JSON, stat *executionStats) {
	logger.Debug(p.Name, ": Starting", startingStage)
	d := []byte(StartSignal)

	if p.RecordStats {
		stat.recordExecution(func() {
			startingStage.ProcessData(d, outputChan, killChan)
		})
		stat.recordExecution(func() {
			startingStage.Finish(outputChan, killChan)
		})
	} else {
		startingStage.ProcessData(d, outputChan, killChan)
		startingStage.Finish(outputChan, killChan)
	}
	p.wg.Done()
}

// Reference: http://blog.golang.org/pipelines
func (p *Pipeline) mergeChans(inputs []chan data.JSON, output chan data.JSON) {
	logger.Debug(p.Name, ": merging channels", inputs, "into channel", output)

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

func (p *Pipeline) branchChans(input chan data.JSON, numCopies int) []chan data.JSON {
	outputs := initDataChans(numCopies)
	logger.Debug(p.Name, ": branching channel", input, "into channels", outputs)

	// Receive all data from input, sending the data receieved
	// on to all the outputs
	go func() {
		for d := range input {
			for _, out := range outputs {
				// Make a copy to ensure concurrent stages
				// can alter data as needed.
				dc := make(data.JSON, len(d))
				copy(dc, d)
				out <- dc
			}
		}
		// Once all data is received from input, also close all the outputs
		for _, out := range outputs {
			close(out)
		}
	}()

	return outputs
}

// Given the original channel being sent to c, provide a new channel that
// can be sent to, where function foo will be performed before the data is passed
// along onto that original channel c. c will be closed after newc is closed.
// This logic was created for recording stats on data sends.
func (p *Pipeline) interceptChan(c chan data.JSON, foo func(d data.JSON)) (newc chan data.JSON) {
	// We still need to bridge communication with a new channel since
	// the sender will be closing the channel before we can send
	newc = make(chan data.JSON)
	logger.Debug(p.Name, ": intercepting channel", c, "into channel", newc)

	go func() {
		for d := range newc {
			foo(d)
			c <- d
		}
		close(c)
	}()
	return
}

func initDataChans(length int) []chan data.JSON {
	cs := make([]chan data.JSON, length)
	for i := range cs {
		cs[i] = make(chan data.JSON)
	}
	return cs
}

func handleInterrupt(killChan chan error) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			killChan <- errors.New("Exiting due to interrupt signal.")
		}
	}()
}

// Stats returns a string (formatted for output display) listing the stats
// gathered for each stage executed.
// Note RecordStats must be set to true for full stats to be available.
func (p *Pipeline) Stats() string {
	o := fmt.Sprintf("%s: %s\r\n", p.Name, p.timer)
	if p.RecordStats {
		for i, stages := range p.Stages {
			o += fmt.Sprintf("Stage %d)\r\n", i+1)
			for j, stage := range stages {
				o += fmt.Sprintf("  * %v\r\n", stage)
				stat := p.stats[i][j]
				stat.calculate()
				o += fmt.Sprintf("     - Total/Avg Execution Time = %f/%fs\r\n", stat.totalExecutionTime, stat.avgExecutionTime)
				o += fmt.Sprintf("     - Payloads Sent/Received = %d/%d\r\n", stat.dataSentCounter, stat.dataReceivedCounter)
				o += fmt.Sprintf("     - Total/Avg Bytes Sent = %d/%d\r\n", stat.totalBytesSent, stat.avgBytesSent)
				o += fmt.Sprintf("     - Total/Avg Bytes Received = %d/%d\r\n", stat.totalBytesReceived, stat.avgBytesReceived)
			}
		}
	}
	return o
}
