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
	Name         string // Name is simply for display purpsoses in log output.
	BufferLength int    // Set to control channel buffering, default is 8.
	Stages       [][]PipelineStage
	stats        [][]*executionStats
	dataHandlers [][]*dataHandlerQueue
	timer        *util.Timer
	wg           sync.WaitGroup
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
	p := &Pipeline{Name: "Pipeline", Stages: stageSlices, BufferLength: 8}

	// setup stats to match the stages
	p.stats = make([][]*executionStats, len(stageSlices))
	for i, ss := range stageSlices {
		p.stats[i] = make([]*executionStats, len(ss))
		for j := range ss {
			p.stats[i][j] = &executionStats{}
		}
	}
	// Setup data hanlders to match the stages
	// This is related to Concurrently processing PipelineStage's.
	// See ConcurrentPipelineStage.
	p.dataHandlers = make([][]*dataHandlerQueue, len(stageSlices))
	for i, ss := range stageSlices {
		p.dataHandlers[i] = make([]*dataHandlerQueue, len(ss))
		for j, s := range ss {
			if IsConcurrent(s) {
				size := s.(ConcurrentPipelineStage).Concurrency()
				if size > 0 {
					p.dataHandlers[i][j] = newdataHandlerQueue(size)
				} else {
					p.dataHandlers[i][j] = nil
				}
			} else {
				p.dataHandlers[i][j] = nil
			}
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

	dataChans := p.initDataChans(len(p.Stages) - 1)
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
	nonStartingDataHandlers := p.dataHandlers[1:]
	for i, stages := range nonStartingStages {
		inputChan := dataChans[i]
		var outputChan chan data.JSON
		if i < len(nonStartingStages)-1 {
			outputChan = dataChans[i+1]
		}
		stats := nonStartingStats[i]
		dhs := nonStartingDataHandlers[i]

		logger.Debug(p.Name, ": ", stages, "has inputChan", inputChan, "/ outputChan", outputChan)

		// Check if we're setting up a branching vs non-branching stage
		if len(stages) > 1 {
			// Setup branching and merging
			inputs := p.branchChans(inputChan, len(stages))
			// no merging needed for final stage
			if outputChan != nil {
				outputs := p.initDataChans(len(stages))
				p.mergeChans(outputs, outputChan)
				for j, s := range stages {
					out := outputs[j]
					stat := stats[j]
					dh := dhs[j]
					// intercept so we can record stats on sent data
					out = p.interceptChan(outputs[j], func(d data.JSON) {
						stat.recordDataSent(d)
					})
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[j], out, dh, stat)
					p.wg.Add(1)
				}
			} else {
				for j, s := range stages {
					// Each stage runs in it's own goroutine
					go p.processStage(s, killChan, inputs[j], nil, dhs[j], stats[j])
					p.wg.Add(1)
				}
			}
		} else {
			// If there's just a single Stage to run, no need to set up
			// extra channels for branching/merging.
			go p.processStage(stages[0], killChan, inputChan, outputChan, dhs[0], stats[0])
			p.wg.Add(1)
		}
	}

	p.setupStartingStages(killChan, dataChans[0])
}

func (p *Pipeline) processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan data.JSON, dh *dataHandlerQueue, stat *executionStats) {
	logger.Debug(p.Name, ": Stage", stage, "waiting for data on chan", inputChan)

	for d := range inputChan {
		logger.Info("Pipeline: Stage", stage, "receiving data:", len(d), "bytes")
		// logger.Debug(string(d))
		stat.recordDataReceived(d)
		if dh != nil {
			dh.processData(stage, d, outputChan, killChan, stat)
		} else {
			stat.recordExecution(func() {
				stage.ProcessData(d, outputChan, killChan)
			})
		}
	}

	if dh != nil {
		// Let the data handler know input is closed, and then
		// wait for any outstanding data handling goroutines
		// to finish.
		dh.inputClosed = true
		<-dh.doneChan
	}

	logger.Debug(p.Name, ": Stage", stage, "finishing...")
	stat.recordExecution(func() {
		stage.Finish(outputChan, killChan)
	})
	logger.Info(p.Name, ": Stage", stage, "finished.", p.timer)
	p.wg.Done()
}

func (p *Pipeline) setupStartingStages(killChan chan error, outputChan chan data.JSON) {
	startingStages := p.Stages[0]
	stats := p.stats[0]
	dataHandlers := p.dataHandlers[0]
	// Setup the Starter stage and kick off the pipeline execution
	// The Starter also runs in it's own goroutine
	if len(startingStages) > 1 {
		outputs := p.initDataChans(len(startingStages))
		p.mergeChans(outputs, outputChan)
		for i, s := range startingStages {
			out := outputs[i]
			stat := stats[i]
			dh := dataHandlers[i]
			// intercept so we can record stats on sent data
			out = p.interceptChan(outputs[i], func(d data.JSON) {
				stat.recordDataSent(d)
			})
			// Each stage runs in it's own goroutine
			go p.processStartingStage(s, killChan, out, dh, stat)
			p.wg.Add(1)
		}
	} else {
		// In the single-starter case, no need to create extra channels
		// for branching/merging.
		out := outputChan
		stat := stats[0]
		dh := dataHandlers[0]

		// intercept so we can record stats on sent data
		out = p.interceptChan(outputChan, func(d data.JSON) {
			stat.recordDataSent(d)
		})
		go p.processStartingStage(startingStages[0], killChan, out, dh, stat)
		p.wg.Add(1)
	}
}

func (p *Pipeline) processStartingStage(startingStage PipelineStage, killChan chan error, outputChan chan data.JSON, dh *dataHandlerQueue, stat *executionStats) {
	logger.Debug(p.Name, ": Starting", startingStage)
	d := []byte(StartSignal)

	if dh != nil {
		dh.processData(startingStage, d, outputChan, killChan, stat)
	} else {
		stat.recordExecution(func() {
			startingStage.ProcessData(d, outputChan, killChan)
		})
	}

	if dh != nil {
		// Let the data handler know input is closed, and then
		// wait for any outstanding data handling goroutines
		// to finish.
		dh.inputClosed = true
		<-dh.doneChan
	}

	stat.recordExecution(func() {
		startingStage.Finish(outputChan, killChan)
	})
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
	outputs := p.initDataChans(numCopies)
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
	newc = p.initDataChan()
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

func (p *Pipeline) initDataChans(length int) []chan data.JSON {
	cs := make([]chan data.JSON, length)
	for i := range cs {
		cs[i] = p.initDataChan()
	}
	return cs
}
func (p *Pipeline) initDataChan() chan data.JSON {
	return make(chan data.JSON, p.BufferLength)
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
func (p *Pipeline) Stats() string {
	o := fmt.Sprintf("%s: %s\r\n", p.Name, p.timer)
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
	return o
}
