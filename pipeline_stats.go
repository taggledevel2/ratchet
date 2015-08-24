package ratchet

import (
	"time"

	"github.com/DailyBurn/ratchet/data"
)

type ExecutionStat struct {
	dataSentCounter     int
	dataReceivedCounter int
	executionsCounter   int
	totalExecutionTime  float64
	avgExecutionTime    float64
	totalBytesReceived  int
	avgBytesReceived    int
	totalBytesSent      int
	avgBytesSent        int
}

func (s *ExecutionStat) recordExecution(foo func()) {
	s.executionsCounter++
	st := time.Now()
	foo()
	s.totalExecutionTime += time.Now().Sub(st).Seconds()
}

func (s *ExecutionStat) recordDataSent(d data.JSON) {
	s.dataSentCounter++
	s.totalBytesSent += len(d)
}

func (s *ExecutionStat) recordDataReceived(d data.JSON) {
	s.dataReceivedCounter++
	s.totalBytesReceived += len(d)
}

func (s *ExecutionStat) calculate() {
	if s.executionsCounter > 0 {
		s.avgExecutionTime = (s.totalExecutionTime / float64(s.executionsCounter))
	}
	if s.dataReceivedCounter > 0 {
		s.avgBytesReceived = (s.totalBytesReceived / s.dataReceivedCounter)
	}
	if s.dataSentCounter > 0 {
		s.avgBytesSent = (s.totalBytesSent / s.dataSentCounter)
	}
}
