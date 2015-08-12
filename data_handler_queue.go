package ratchet

import (
	"container/list"
	"sync"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
)

type dataHandlerQueue struct {
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

func newdataHandlerQueue(size int) *dataHandlerQueue {
	return &dataHandlerQueue{workThrottle: make(chan workSignal, size), workList: list.New(), doneChan: make(chan bool), inputClosed: false}
}

func (q *dataHandlerQueue) processData(stage PipelineStage, d data.JSON, outputChan chan data.JSON, killChan chan error, stat *executionStats) {
	logger.Debug("dataHandlerQueue: processData", stage, "waiting for work")
	// wait for room in the queue
	q.workThrottle <- workSignal{}
	logger.Debug("dataHandlerQueue: processData", stage, "work obtained")
	rc := make(chan data.JSON)
	done := make(chan bool)
	// setup goroutine to handle result
	go func() {
		res := result{outputChan: outputChan, data: []data.JSON{}, open: true}
		q.mutex.Lock()
		q.workList.PushBack(&res)
		q.mutex.Unlock()
		logger.Debug("dataHandlerQueue: processData", stage, "waiting to receive data on result chan")
		for {
			select {
			case d, open := <-rc:
				logger.Debug("dataHandlerQueue: processData", stage, "received data on result chan")
				res.data = append(res.data, d)
				// outputChan will need to be closed if the rc chan was closed by the stage
				res.open = open
			case <-done:
				res.done = true
				logger.Debug("dataHandlerQueue: processData", stage, "done, releasing work")
				<-q.workThrottle
				q.sendResults()
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
}

// sendResults handles sending work that is completed, as well as
// guaranteeing a FIFO order of the resulting data sent over the
// original outputChan.
func (q *dataHandlerQueue) sendResults() {
	q.mutex.Lock()
	logger.Debug("dataHandlerQueue: sendResults checking for valid data to send")
	e := q.workList.Front()
	for e != nil && e.Value.(*result).done {
		logger.Debug("dataHandlerQueue: sendResults sending data")
		res := q.workList.Remove(e).(*result)
		for _, d := range res.data {
			res.outputChan <- d
		}
		if !res.open {
			logger.Debug("dataHandlerQueue: sendResults closing outputChan")
			close(res.outputChan)
		}
		e = q.workList.Front()
	}
	q.mutex.Unlock()

	if q.inputClosed && q.workList.Len() == 0 {
		q.doneChan <- true
	}
}
