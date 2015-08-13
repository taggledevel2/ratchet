package ratchet

import (
	"container/list"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
)

// dataHandler encapsulates the logic around how received data is processed
// by a PipelineStage. The majority of code in dataHandler is
// specifically related to ConcurrentPipelineStage processing, where
// data is handled in a queue/pool fashion in multiple goroutines.
// If no concurrency is needed, then dataHandler simply calls
// PipelineStage's ProcessData without dealing with any concurrency
// management.
type dataHandler struct {
	concurrency  int
	workThrottle chan workSignal
	workList     *list.List
	mutex        sync.Mutex
	doneChan     chan bool
	inputClosed  bool
}

type workSignal struct{}

type result struct {
	done       bool
	data       []data.JSON
	outputChan chan data.JSON
	open       bool
}

func newDataHandler(concurrency int) *dataHandler {
	if concurrency < 1 {
		return &dataHandler{concurrency: 0}
	}
	return &dataHandler{concurrency: concurrency, workThrottle: make(chan workSignal, concurrency), workList: list.New(), doneChan: make(chan bool), inputClosed: false}
}

func (dh *dataHandler) processData(stage PipelineStage, d data.JSON, outputChan chan data.JSON, killChan chan error, stat *executionStats) {
	// If no concurrency is needed, simply call stage.ProcessData and return...
	if dh.concurrency == 0 {
		logger.Debug("dataHandler: processData", stage, "without concurrency")
		stat.recordExecution(func() {
			stage.ProcessData(d, outputChan, killChan)
		})
		return
	}

	// ... otherwise process the data in a concurrent queue/pool of goroutines
	logger.Debug("dataHandler: processData", stage, "waiting for work")
	// wait for room in the queue
	dh.workThrottle <- workSignal{}
	logger.Debug("dataHandler: processData", stage, "work obtained")
	rc := make(chan data.JSON)
	done := make(chan bool)
	// setup goroutine to handle result
	go func() {
		res := result{outputChan: outputChan, data: []data.JSON{}, open: true}
		dh.mutex.Lock()
		dh.workList.PushBack(&res)
		dh.mutex.Unlock()
		logger.Debug("dataHandler: processData", stage, "waiting to receive data on result chan")
		for {
			select {
			case d, open := <-rc:
				logger.Debug("dataHandler: processData", stage, "received data on result chan")
				res.data = append(res.data, d)
				// outputChan will need to be closed if the rc chan was closed by the stage
				res.open = open
			case <-done:
				res.done = true
				logger.Debug("dataHandler: processData", stage, "done, releasing work")
				<-dh.workThrottle
				dh.sendResults()
				return
			}
		}
	}()
	// do normal data processing, passing in new result chan
	// instead of the original outputChan
	go stat.recordExecution(func() {
		stage.ProcessData(d, rc, killChan)
		done <- true
	})

	// wait on done chan to return
	<-done
}

// sendResults handles sending work that is completed, as well as
// guaranteeing a FIFO order of the resulting data sent over the
// original outputChan.
func (dh *dataHandler) sendResults() {
	dh.mutex.Lock()
	logger.Debug("dataHandler: sendResults checking for valid data to send")
	e := dh.workList.Front()
	for e != nil && e.Value.(*result).done {
		logger.Debug("dataHandler: sendResults sending data")
		res := dh.workList.Remove(e).(*result)
		for _, d := range res.data {
			res.outputChan <- d
		}
		if !res.open {
			logger.Debug("dataHandler: sendResults closing outputChan")
			close(res.outputChan)
		}
		e = dh.workList.Front()
	}
	dh.mutex.Unlock()

	if dh.inputClosed && dh.workList.Len() == 0 {
		dh.doneChan <- true
	}
}
