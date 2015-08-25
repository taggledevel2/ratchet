package ratchet

import (
	"fmt"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
)

// DataProcessor is the interface that should be implemented to perform data-related
// tasks within a Pipeline. DataProcessors are responsible for receiving, processing,
// and then sending data on to the next stage of processing.
type DataProcessor interface {
	// ProcessData will be called for each data sent from the previous stage.
	// ProcessData is called with a data.JSON instance, which is the data being received,
	// an outputChan, which is the channel to send data to, and a killChan,
	// which is a channel to send unexpected errors to (halting execution of the Pipeline).
	ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error)

	// Finish will be called after the previous stage has finished sending data,
	// and no more data will be received by this DataProcessor. Often times
	// Finish can be an empty function implementation, but sometimes it is
	// necessary to perform final data processing.
	Finish(outputChan chan data.JSON, killChan chan error)
}

// ConcurrentDataProcessor is a DataProcessor that also defines
// a level of concurrency. For example, if Concurrency() returns 2,
// then the pipeline will allow the stage to execute up to 2 ProcessData()
// calls concurrently.
//
// Note that the order of data processing is maintained, meaning that
// when a DataProcessor receives ProcessData calls d1, d2, ..., the resulting data
// payloads sent on the outputChan will be sent in the same order as received.
type ConcurrentDataProcessor interface {
	DataProcessor
	Concurrency() int
}

// IsConcurrent returns true if the given DataProcessor implements ConcurrentDataProcessor
func isConcurrent(p DataProcessor) bool {
	_, ok := interface{}(p).(ConcurrentDataProcessor)
	return ok
}

// dataProcessor is a type used internally to the Pipeline management
// code, and wraps a DataProcessor instance. DataProcessor is the main
// interface that should be implemented to perform work within the data
// pipeline, and this dataProcessor type simply embeds it and adds some
// helpful channel management and other attributes.
type dataProcessor struct {
	DataProcessor
	outputs    []DataProcessor
	inputChan  chan data.JSON
	outputChan chan data.JSON
	brancher   *chanBrancher
	merger     *chanMerger
	// ExecutionStat
	// dataHandler
	// timer util.Timer
}

type chanBrancher struct {
	outputChans []chan data.JSON
}

func (b *chanBrancher) branchOut(fromChan chan data.JSON) {
	go func() {
		for d := range fromChan {
			for _, out := range b.outputChans {
				// Make a copy to ensure concurrent stages
				// can alter data as needed.
				dc := make(data.JSON, len(d))
				copy(dc, d)
				out <- dc
			}
		}
		// Once all data is received, also close all the outputs
		for _, out := range b.outputChans {
			logger.Debug(b, "branchOut closing", out)
			close(out)
		}
	}()
}

type chanMerger struct {
	inputChans []chan data.JSON
	sync.WaitGroup
}

func (m *chanMerger) mergeIn(toChan chan data.JSON) {
	// Start an output goroutine for each input channel.
	mergeData := func(c chan data.JSON) {
		for d := range c {
			toChan <- d
		}
		m.Done()
	}
	m.Add(len(m.inputChans))
	for _, c := range m.inputChans {
		go mergeData(c)
	}

	go func() {
		logger.Debug(m, "mergeIn waiting")
		m.Wait()
		logger.Debug(m, "mergeIn closing", toChan)
		close(toChan)
	}()
}

// Do takes a DataProcessor instance and returns the dataProcessor
// type that will wrap it for internal ratchet processing. The details
// of the dataProcessor wrapper type are abstracted away from the
// implementing end-user code. The "Do" function is named
// succinctly to provide a nicer syntax when creating a PipelineLayout.
// See the ratchet package documentation for code examples of creating
// a new branching pipeline layout.
func Do(processor DataProcessor) *dataProcessor {
	dp := dataProcessor{DataProcessor: processor}
	dp.outputChan = make(chan data.JSON)
	dp.inputChan = make(chan data.JSON)
	return &dp
}

// Outputs should be called to specify which DataProcessor instances the current
// processor should send it's output to. See the ratchet package
// documentation for code examples and diagrams.
func (dp *dataProcessor) Outputs(processors ...DataProcessor) *dataProcessor {
	dp.outputs = processors
	return dp
}

// pass through String output to the DataProcessor
func (dp *dataProcessor) String() string {
	return fmt.Sprintf("%v", dp.DataProcessor)
}
