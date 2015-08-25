package ratchet

import (
	"errors"
	"os"
	"os/signal"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// StartSignal is what's sent to a starting DataProcessor
// to kick off execution. Typically this value will be ignored.
var StartSignal = "GO"

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	layout       *PipelineLayout
	Name         string // Name is simply for display purpsoses in log output.
	BufferLength int    // Set to control channel buffering, default is 8.
	PrintData    bool   // Set to true to log full data payloads (only in Debug logging mode).
	timer        *util.Timer
	wg           sync.WaitGroup
}

// NewPipeline creates a new pipeline ready to run the given DataProcessors.
// For more complex use-cases, see NewBranchingPipeline.
func NewPipeline(processors ...DataProcessor) *Pipeline {
	p := &Pipeline{Name: "Pipeline"}
	stages := make([]*PipelineStage, len(processors))
	for i, p := range processors {
		dp := Do(p)
		if i < len(processors)-1 {
			dp.Outputs(processors[i+1])
		}
		stages = append(stages, NewPipelineStage([]*dataProcessor{dp}...))
	}
	p.layout, _ = NewPipelineLayout(stages...)
	return p
}

// NewBranchingPipeline creates a new pipeline ready to run the
// given PipelineLayout, which can accomodate branching/merging
// between stages each containing variable number of DataProcessors.
// See the ratchet package documentation for code examples and diagrams.
func NewBranchingPipeline(layout *PipelineLayout) *Pipeline {
	p := &Pipeline{layout: layout, Name: "Pipeline"}
	return p
}

// In order to support the branching PipelineLayout creation syntax, the
// dataProcessor.outputs are "DataProcessor" interface types, and not the "dataProcessor"
// wrapper types. This function loops through the layout and matches the
// interface to wrapper objects and returns them.
func (p *Pipeline) dataProcessorOutputs(dp *dataProcessor) []*dataProcessor {
	dpouts := make([]*dataProcessor, len(dp.outputs))
	for i := range dp.outputs {
		for _, stage := range p.layout.stages {
			for j := range stage.processors {
				if dp.outputs[i] == stage.processors[j].DataProcessor {
					dpouts[i] = stage.processors[j]
				}
			}
		}
	}
	return dpouts
}

// At this point in pipeline initialization, every dataProcessor has an input
// and output channel, but there is nothing connecting them together. In order
// to support branching and merging between stages (as defined by each
// dataProcessor's outputs), we set up some intermediary channels that will
// manage copying and passing data between stages, as well as properly closing
// channels when all data is received.
func (p *Pipeline) connectStages() {
	logger.Debug(p.Name, ": connecting stages")
	// First, setup the bridgeing channels & brancher/merger's to aid in
	// managing channel communication between processors.
	for _, stage := range p.layout.stages {
		for _, from := range stage.processors {
			if from.outputs != nil {
				from.brancher = &chanBrancher{outputChans: []chan data.JSON{}}
				for _, to := range p.dataProcessorOutputs(from) {
					if to.merger == nil {
						to.merger = &chanMerger{inputChans: []chan data.JSON{}}
					}
					c := p.initDataChan()
					from.brancher.outputChans = append(from.brancher.outputChans, c)
					to.merger.inputChans = append(to.merger.inputChans, c)
				}
			}
		}
	}
	// Loop through again and setup goroutines to handle data management
	// between the branchers and mergers
	for _, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			if dp.brancher != nil {
				dp.brancher.branchOut(dp.outputChan)
			}
			if dp.merger != nil {
				dp.merger.mergeIn(dp.inputChan)
			}
		}
	}
}

func (p *Pipeline) runStages(killChan chan error) {
	for n, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			p.wg.Add(1)
			// Each DataProcessor runs in a separate gorountine.
			go func(n int, dp *dataProcessor) {
				// This is where the main DataProcessor interface
				// functions are called.
				logger.Info(p.Name, ": stage", n+1, "DataProcessor", dp, "waiting to receive data")
				for d := range dp.inputChan {
					logger.Info(p.Name, ": stage", n+1, "DataProcessor", dp, "received data")
					logger.Debug(p.Name, ": stage", n+1, "DataProcessor", dp, "data =", string(d))
					dp.ProcessData(d, dp.outputChan, killChan)
				}
				logger.Info(p.Name, ": stage", n+1, "DataProcessor", dp, "input channel closed, calling Finish")
				dp.Finish(dp.outputChan, killChan)
				if dp.outputChan != nil {
					logger.Info(p.Name, ": stage", n+1, "DataProcessor", dp, "closing output channel")
					close(dp.outputChan)
				}
				p.wg.Done()
			}(n, dp)
		}
	}
}

// Run finalizes the channel connections between PipelineStages
// and kicks off execution.
// Run will return a killChan that should be waited on so your calling function doesn't
// return prematurely. Any stage of the pipeline can send to the killChan to halt
// execution. Your calling function should check if the sent value is an error or nil to know if
// execution was a failure or a success (nil being the success value).
func (p *Pipeline) Run() (killChan chan error) {
	p.timer = util.StartTimer()
	killChan = make(chan error)

	p.connectStages()
	p.runStages(killChan)

	for _, dp := range p.layout.stages[0].processors {
		logger.Debug(p.Name, ": sending", StartSignal, "to", dp)
		dp.inputChan <- data.JSON(StartSignal)
		dp.Finish(dp.outputChan, killChan)
		close(dp.inputChan)
	}

	// After all the stages are running, send the StartSignal
	// to the initial stage processors to kick off execution, and
	// then wait until all the processing goroutines are done to
	// signal successful pipeline completion.
	go func() {
		p.wg.Wait()
		p.timer.Stop()
		killChan <- nil
	}()

	handleInterrupt(killChan)

	return killChan
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

// func (p *Pipeline) String() string {
// 	// Print an overview of the pipeline
// 	stageNames := []string{}
// 	for _, s := range p.layout.stages {
// 		stageNames = append(stageNames, fmt.Sprintf("%v", s))
// 	}
// 	return p.Name + ": " + strings.Join(stageNames, " -> "))
// }

func handleInterrupt(killChan chan error) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			killChan <- errors.New("Exiting due to interrupt signal.")
		}
	}()
}

// // Stats returns a string (formatted for output display) listing the stats
// // gathered for each stage executed.
// func (p *Pipeline) Stats() string {
// 	o := fmt.Sprintf("%s: %s\r\n", p.Name, p.timer)
// 	for i, stages := range p.Stages {
// 		o += fmt.Sprintf("Stage %d)\r\n", i+1)
// 		for j, stage := range stages {
// 			o += fmt.Sprintf("  * %v\r\n", stage)
// 			stat := p.stats[i][j]
// 			stat.calculate()
// 			o += fmt.Sprintf("     - Total/Avg Execution Time = %f/%fs\r\n", stat.totalExecutionTime, stat.avgExecutionTime)
// 			o += fmt.Sprintf("     - Payloads Sent/Received = %d/%d\r\n", stat.dataSentCounter, stat.dataReceivedCounter)
// 			o += fmt.Sprintf("     - Total/Avg Bytes Sent = %d/%d\r\n", stat.totalBytesSent, stat.avgBytesSent)
// 			o += fmt.Sprintf("     - Total/Avg Bytes Received = %d/%d\r\n", stat.totalBytesReceived, stat.avgBytesReceived)
// 		}
// 	}
// 	return o
// }
