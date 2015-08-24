package ratchet

import (
	"fmt"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
)

// StartSignal is what's sent to a starting PipelineStage's ProcessData.
// Typically this value will be ignored.
var StartSignal = "GO"

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	layout       *PipelineLayout
	Name         string // Name is simply for display purpsoses in log output.
	BufferLength int    // Set to control channel buffering, default is 8.
	PrintData    bool   // Set to true to log full data payloads (only in Debug logging mode).
	// util.Timer
	wg sync.WaitGroup
}

func NewPipeline(processors ...DataProcessor) *Pipeline {
	p := &Pipeline{}
	stages := make([]*PipelineStage, len(processors))
	for i, p := range processors {
		dp := Do(p)
		if i < len(processors)-1 {
			dp.Outputs(processors[i+1])
		}
		stages = append(stages, NewPipelineStage([]*dataProcessor{dp}...))
	}
	p.layout, _ = NewPipelineLayout(stages...)
	p.connectChans()
	return p
}

func NewBranchingPipeline(layout *PipelineLayout) *Pipeline {
	p := &Pipeline{layout: layout}
	p.connectChans()
	return p
}

type PipelineLayout struct {
	stages []*PipelineStage
}

func NewPipelineLayout(stages ...*PipelineStage) (*PipelineLayout, error) {
	l := &PipelineLayout{stages}
	if err := l.validate(); err != nil {
		return nil, err
	}
	return l, nil
}

// validate returns error or nil
// A valid PipelineLayout meets these conditions:
// 1) final stages must NOT have outputs set
// 2) non-final stages must HAVE outputs set
// 3) outputs must point to a DataProcessor in the next immediate stage
// 4) a non-starting DataProcessor must be pointed to by one of the previous outputs
func (l *PipelineLayout) validate() error {
	logger.Debug("PipelineLayout: validating")
	var stage *PipelineStage
	for stageNum := range l.stages {
		stage = l.stages[stageNum]
		var dp *dataProcessor
		for j := range stage.processors {
			dp = stage.processors[j]
			// 1) final stages must NOT have outputs set
			// 2) non-final stages must HAVE outputs set
			if stageNum == len(l.stages)-1 && dp.outputs != nil {
				return fmt.Errorf("DataProcessor (%v) must have Outputs set in final PipelineStage", dp)
			} else if stageNum != len(l.stages)-1 && dp.outputs == nil {
				return fmt.Errorf("DataProcessor (%v) must have Outputs set in non-final PipelineStage #%d", dp, stageNum+1)
			}
			// 3) outputs must point to a DataProcessor in the next immediate stage
			if stageNum < len(l.stages)-1 {
				nextStage := l.stages[stageNum+1]
				for k := range dp.outputs {
					if !nextStage.hasProcessor(dp.outputs[k]) {
						return fmt.Errorf("DataProcessor (%v) Outputs must point to DataProcessor in the next PipelineStage #%d", dp, stageNum+2)
					}
				}
			}
			// 4) a non-starting DataProcessor must be pointed to by one of the previous outputs
			if stageNum > 0 {
				prevStage := l.stages[stageNum-1]
				if !prevStage.hasOutput(dp.DataProcessor) {
					return fmt.Errorf("DataProcessor (%v) is not pointed to by any output in the previous PipelineStage #%d", dp, stageNum)
				}
			}
		}
	}
	return nil
}

type PipelineStage struct {
	processors []*dataProcessor
}

func NewPipelineStage(processors ...*dataProcessor) *PipelineStage {
	return &PipelineStage{processors}
}

func (s *PipelineStage) hasProcessor(p DataProcessor) bool {
	for i := range s.processors {
		if s.processors[i].DataProcessor == p {
			return true
		}
	}
	return false
}

func (s *PipelineStage) hasOutput(p DataProcessor) bool {
	for i := range s.processors {
		for j := range s.processors[i].outputs {
			if s.processors[i].outputs[j] == p {
				return true
			}
		}
	}
	return false
}

type dataProcessor struct {
	DataProcessor
	outputs    []DataProcessor
	inputChan  chan data.JSON
	outputChan chan data.JSON
	pointedTo  bool // validation helper
	// ExecutionStat
	// dataHandler
	// timer util.Timer
}

func Do(processor DataProcessor) *dataProcessor {
	dp := dataProcessor{DataProcessor: processor}
	dp.outputChan = make(chan data.JSON)
	dp.inputChan = make(chan data.JSON)
	return &dp
}

func (dp *dataProcessor) Outputs(processors ...DataProcessor) *dataProcessor {
	dp.outputs = processors
	return dp
}

// pass through String output to the DataProcessor
func (dp *dataProcessor) String() string {
	return fmt.Sprintf("%v", dp.DataProcessor)
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

func (p *Pipeline) connectChans() {
	logger.Debug(p.Name, ": connecting channels")
	for _, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			if dp.outputs != nil {
				p.bridge(dp, p.dataProcessorOutputs(dp))
			}
		}
	}
}

func (p *Pipeline) bridge(fromProc *dataProcessor, toProcs []*dataProcessor) {
	logger.Debug(fmt.Sprintf(p.Name, ": bridge %v -> %v", fromProc, toProcs))
	bridgeChan := make(chan data.JSON)
	// for i := range toProcs {
	// 	toProcs[i].inputChan = c
	// }

	go func() {
		logger.Debug(p.Name, ": bridge", fromProc, "- waiting to receive data")
		for d := range fromProc.outputChan {
			// logger.Debug(p.Name, ": bridge", fromProc, "- copying and sending data")
			// logger.Debug("  data =", string(d))
			dc := make(data.JSON, len(d))
			copy(dc, d)
			bridgeChan <- dc
		}
		logger.Debug(p.Name, ": bridge", fromProc, "- closing bridge chan")
		close(bridgeChan)
	}()
}

// Reference: http://blog.golang.org/pipelines
// func (p *Pipeline) merge(inputs []chan data.JSON, output chan data.JSON) {
// 	logger.Debug(p.Name, ": merging channels", inputs, "into channel", output)
//
// 	var wg sync.WaitGroup
//
// 	// Start an output goroutine for each input channel in cs.  output
// 	// copies values from c to out until c is closed, then calls wg.Done.
// 	mergeData := func(c chan data.JSON) {
// 		for n := range c {
// 			output <- n
// 		}
// 		wg.Done()
// 	}
// 	wg.Add(len(inputs))
// 	for _, c := range inputs {
// 		go mergeData(c)
// 	}
//
// 	// Start a goroutine to close out once all the output goroutines are
// 	// done.  This must start after the wg.Add call.
// 	go func() {
// 		wg.Wait()
// 		close(output)
// 	}()
// }

func (p *Pipeline) runStages(killChan chan error) {
	for n, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			p.wg.Add(1)
			logger.Info(p.Name, ": stage", n, "running DataProcessor", dp)
			// Each DataProcessor runs in a separate gorountine.
			go func(n int, dp *dataProcessor) {
				// This is where the main DataProcessor interface
				// functions are called.
				logger.Info(p.Name, ": stage", n, "DataProcessor", dp, "waiting to receive data")
				for d := range dp.inputChan {
					logger.Info(p.Name, ": stage", n, "DataProcessor", dp, "received data")
					logger.Debug("  data =", string(d))
					dp.ProcessData(d, dp.outputChan, killChan)
				}
				logger.Info(p.Name, ": stage", n, "DataProcessor", dp, "input channel closed, calling Finish")
				dp.Finish(dp.outputChan, killChan)
				if dp.outputChan != nil {
					logger.Info(p.Name, ": stage", n, "DataProcessor", dp, "closing output channel")
					close(dp.outputChan)
				}
				p.wg.Done()
			}(n, dp)
		}
	}
}

func (p *Pipeline) Run() (killChan chan error) {
	killChan = make(chan error)

	p.runStages(killChan)

	// After all the stages are running, send the StartSignal
	// to the initial stage processors to kick off execution, and
	// then wait until all the processing goroutines are done to
	// signal successful pipeline completion.
	go func() {
		for _, dp := range p.layout.stages[0].processors {
			logger.Debug(p.Name, ": sending", StartSignal, "to", dp)
			dp.inputChan <- data.JSON(StartSignal)
		}
		p.wg.Wait()
		killChan <- nil
	}()

	return killChan
}

// // Run should be called on a Pipeline object to kick things off. It will setup the
// // data channels and connect them between each stage. After calling Run(), the
// // starting PipelineStage instances will receive a single ProcessData function call where
// // the data payload is StartSignal.
// //
// // Run will return a killChan that should be waited on so your main function doesn't
// // return prematurely. Any stage of the pipeline can send to the killChan to halt
// // execution. Your main() function should check if the sent value is an error or nil to know if
// // execution was a failure or a success (nil being the success value).
// func (p *Pipeline) Run() (killChan chan error) {
// 	p.timer = util.StartTimer()
//
// 	// Print an overview of the pipeline
// 	stageNames := []string{}
// 	for _, s := range p.Stages {
// 		stageNames = append(stageNames, fmt.Sprintf("%v", s))
// 	}
// 	logger.Status(p.Name, ": running", strings.Join(stageNames, " -> "))
//
// 	// Each stage should be connected by a separate channel object,
// 	// so that when close()'d it will cut off the processing in a left
// 	// to right manner.
// 	// Example:
// 	//	Pipeline: |StarterStage| dataChan0-> |Stage0| dataChan1-> |Stage1| -> killChan
//
// 	dataChans := p.initDataChans(len(p.Stages) - 1)
// 	killChan = make(chan error)
// 	logger.Debug(p.Name, ": data channels", dataChans)
// 	handleInterrupt(killChan)
//
// 	p.setupStages(killChan, dataChans)
//
// 	go func() {
// 		p.wg.Wait()
// 		logger.Status(p.Name, ": Exiting pipeline.")
// 		logger.Status(p.Name, ":", p.timer.Stop())
// 		killChan <- nil
// 	}()
//
// 	return killChan
// }
//
// func (p *Pipeline) setupStages(killChan chan error, dataChans []chan data.JSON) {
// 	// Setup the channel and data-handling between all the Stages except the
// 	// starting ones (which will be setup a bit differently afterwards).
// 	// (Refer to example above for understanding how indexes are mapped)
// 	nonStartingStages := p.Stages[1:]
// 	nonStartingStats := p.stats[1:]
// 	nonStartingDataHandlers := p.dataHandlers[1:]
// 	for i, stages := range nonStartingStages {
// 		inputChan := dataChans[i]
// 		var outputChan chan data.JSON
// 		if i < len(nonStartingStages)-1 {
// 			outputChan = dataChans[i+1]
// 		}
// 		stats := nonStartingStats[i]
// 		dataHandlers := nonStartingDataHandlers[i]
//
// 		logger.Debug(p.Name, ": ", stages, "has inputChan", inputChan, "/ outputChan", outputChan)
//
// 		// Check if we're setting up a branching vs non-branching stage
// 		if len(stages) > 1 {
// 			// Setup branching and merging
// 			inputs := p.branchChans(inputChan, len(stages))
// 			// no merging needed for final stage
// 			if outputChan != nil {
// 				outputs := p.initDataChans(len(stages))
// 				p.mergeChans(outputs, outputChan)
// 				for j, s := range stages {
// 					out := outputs[j]
// 					stat := stats[j]
// 					// intercept so we can record stats on sent data
// 					out = p.interceptChan(outputs[j], func(d data.JSON) {
// 						stat.recordDataSent(d)
// 					})
// 					// Each stage runs in it's own goroutine
// 					go p.processStage(s, killChan, inputs[j], out, dataHandlers[j], stat)
// 					p.wg.Add(1)
// 				}
// 			} else {
// 				for j, s := range stages {
// 					// Each stage runs in it's own goroutine
// 					go p.processStage(s, killChan, inputs[j], nil, dataHandlers[j], stats[j])
// 					p.wg.Add(1)
// 				}
// 			}
// 		} else {
// 			// If there's just a single Stage to run, no need to set up
// 			// extra channels for branching/merging.
// 			go p.processStage(stages[0], killChan, inputChan, outputChan, dataHandlers[0], stats[0])
// 			p.wg.Add(1)
// 		}
// 	}
//
// 	p.setupStartingStages(killChan, dataChans[0])
// }
//
// func (p *Pipeline) processStage(stage PipelineStage, killChan chan error, inputChan, outputChan chan data.JSON, dataHandler *dataHandler, stat *executionStats) {
// 	logger.Debug(p.Name, ": Stage", stage, "waiting for data on chan", inputChan)
//
// 	for d := range inputChan {
// 		logger.Info(p.Name, ": Stage", stage, "receiving data:", len(d), "bytes")
// 		if p.PrintData {
// 			logger.Debug(string(d))
// 		}
// 		stat.recordDataReceived(d)
// 		dataHandler.processData(stage, d, outputChan, killChan, stat)
// 	}
//
// 	logger.Debug(p.Name, ": Stage", stage, "finishing...")
// 	stat.recordExecution(func() {
// 		stage.Finish(outputChan, killChan)
// 	})
// 	logger.Info(p.Name, ": Stage", stage, "finished.", p.timer)
// 	p.wg.Done()
// }
//
// func (p *Pipeline) setupStartingStages(killChan chan error, outputChan chan data.JSON) {
// 	startingStages := p.Stages[0]
// 	stats := p.stats[0]
// 	dataHandlers := p.dataHandlers[0]
// 	// Setup the Starter stage and kick off the pipeline execution
// 	// The Starter also runs in it's own goroutine
// 	if len(startingStages) > 1 {
// 		outputs := p.initDataChans(len(startingStages))
// 		p.mergeChans(outputs, outputChan)
// 		for i, s := range startingStages {
// 			out := outputs[i]
// 			stat := stats[i]
// 			// intercept so we can record stats on sent data
// 			out = p.interceptChan(outputs[i], func(d data.JSON) {
// 				stat.recordDataSent(d)
// 			})
// 			// Each stage runs in it's own goroutine
// 			go p.processStartingStage(s, killChan, out, dataHandlers[i], stat)
// 			p.wg.Add(1)
// 		}
// 	} else {
// 		// In the single-starter case, no need to create extra channels
// 		// for branching/merging.
// 		out := outputChan
// 		stat := stats[0]
// 		// intercept so we can record stats on sent data
// 		out = p.interceptChan(outputChan, func(d data.JSON) {
// 			stat.recordDataSent(d)
// 		})
// 		go p.processStartingStage(startingStages[0], killChan, out, dataHandlers[0], stat)
// 		p.wg.Add(1)
// 	}
// }
//
// func (p *Pipeline) processStartingStage(startingStage PipelineStage, killChan chan error, outputChan chan data.JSON, dataHandler *dataHandler, stat *executionStats) {
// 	logger.Debug(p.Name, ": Starting", startingStage)
// 	d := []byte(StartSignal)
//
// 	dataHandler.processData(startingStage, d, outputChan, killChan, stat)
//
// 	stat.recordExecution(func() {
// 		startingStage.Finish(outputChan, killChan)
// 	})
// 	p.wg.Done()
// }
//

//
// func (p *Pipeline) branchChans(input chan data.JSON, numCopies int) []chan data.JSON {
// 	outputs := p.initDataChans(numCopies)
// 	logger.Debug(p.Name, ": branching channel", input, "into channels", outputs)
//
// 	// Receive all data from input, sending the data receieved
// 	// on to all the outputs
// 	go func() {
// 		for d := range input {
// 			for _, out := range outputs {
// 				// Make a copy to ensure concurrent stages
// 				// can alter data as needed.
// 				dc := make(data.JSON, len(d))
// 				copy(dc, d)
// 				out <- dc
// 			}
// 		}
// 		// Once all data is received from input, also close all the outputs
// 		for _, out := range outputs {
// 			close(out)
// 		}
// 	}()
//
// 	return outputs
// }
//
// // Given the original channel being sent to c, provide a new channel that
// // can be sent to, where function foo will be performed before the data is passed
// // along onto that original channel c. c will be closed after newc is closed.
// // This logic was created for recording stats on data sends.
// func (p *Pipeline) interceptChan(c chan data.JSON, foo func(d data.JSON)) (newc chan data.JSON) {
// 	// We still need to bridge communication with a new channel since
// 	// the sender will be closing the channel before we can send
// 	newc = p.initDataChan()
// 	logger.Debug(p.Name, ": intercepting channel", c, "into channel", newc)
//
// 	go func() {
// 		for d := range newc {
// 			foo(d)
// 			c <- d
// 		}
// 		close(c)
// 	}()
// 	return
// }
//
// func (p *Pipeline) initDataChans(length int) []chan data.JSON {
// 	cs := make([]chan data.JSON, length)
// 	for i := range cs {
// 		cs[i] = p.initDataChan()
// 	}
// 	return cs
// }
// func (p *Pipeline) initDataChan() chan data.JSON {
// 	return make(chan data.JSON, p.BufferLength)
// }
//
// func handleInterrupt(killChan chan error) {
// 	c := make(chan os.Signal, 1)
// 	signal.Notify(c, os.Interrupt)
// 	go func() {
// 		for range c {
// 			killChan <- errors.New("Exiting due to interrupt signal.")
// 		}
// 	}()
// }
//
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
