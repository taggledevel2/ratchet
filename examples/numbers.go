package main

import (
	"fmt"
	"os"

	"github.com/DailyBurn/ratchet"
	"github.com/DailyBurn/ratchet/stages"
)

// NumberStarter implements the PipelineStarter interface
type NumberStarter struct{}

func (s *NumberStarter) Start(outputChan chan ratchet.Data, exitChan chan error) {
	fmt.Println("[NumberStarter] Start()")
	numbersData := []byte("[35,13,78,22,4,450,2,243,782,44]")
	fmt.Println("[NumberStarter] Sending", numbersData, "to chan", outputChan)
	outputChan <- numbersData
	close(outputChan)
}

func (w *NumberStarter) String() string {
	return "NumberStarter"
}

// NumberStats implements the PipelineStage interface
type NumberStats struct{}

type NumberStatsInput []int
type NumberStatsOutput struct {
	Sum  int
	Mean int
}

func (s *NumberStats) HandleData(data ratchet.Data, outputChan chan ratchet.Data, exitChan chan error) {
	var numbers NumberStatsInput
	err := ratchet.ParseDataIntoStructPtr(data, &numbers)
	if err != nil {
		exitChan <- err
	}
	stats := NumberStatsOutput{Sum: 0}
	for _, n := range numbers {
		stats.Sum += n
	}
	stats.Mean = (stats.Sum / len(numbers))

	output, _ := ratchet.NewDataFromStruct(stats)
	outputChan <- output
}

func (s *NumberStats) Finish(outputChan chan ratchet.Data, exitChan chan error) {
	close(outputChan)
}

func (s *NumberStats) String() string {
	return "NumberStats"
}

func main() {
	// Simple starter that sends a numbers array (defined in this example file).
	numberStarter := &NumberStarter{}
	// Calculate some basic stats on the numbers (defined in this example file).
	numberStats := &NumberStats{}
	// Prints resulting data to stdout (one of the standard ratchet stages).
	writeToStdout := stages.NewIoWriter(os.Stdout)

	pipeline := ratchet.NewPipeline(numberStarter, numberStats, writeToStdout)
	err := <-pipeline.Run()

	if err != nil {
		fmt.Println("pipeline returned err:", err)
	} else {
		fmt.Println("pipeline completed successfully")
	}
}
