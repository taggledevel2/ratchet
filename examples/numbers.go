package main

import (
	"fmt"
	"os"

	"github.com/DailyBurn/ratchet"
	"github.com/DailyBurn/ratchet/stages"
)

// numberStarter implements the PipelineStarter interface
type numberStarter struct{}

func (s *numberStarter) Start(outputChan chan ratchet.Data, killChan chan error) {
	fmt.Println("[numberStarter] Start()")
	numbersData := []byte("[35,13,78,22,4,450,2,243,782,44]")
	fmt.Println("[numberStarter] Sending", numbersData, "to chan", outputChan)
	outputChan <- numbersData
	close(outputChan)
}

func (s *numberStarter) String() string {
	return "numberStarter"
}

// numberStats implements the PipelineStage interface
type numberStats struct{}

type numberStatsInput []int
type numberStatsOutput struct {
	Sum  int
	Mean int
}

func (s *numberStats) HandleData(data ratchet.Data, outputChan chan ratchet.Data, killChan chan error) {
	var numbers numberStatsInput
	err := ratchet.ParseDataIntoStructPtr(data, &numbers)
	if err != nil {
		killChan <- err
	}
	stats := numberStatsOutput{Sum: 0}
	for _, n := range numbers {
		stats.Sum += n
	}
	stats.Mean = (stats.Sum / len(numbers))

	output, _ := ratchet.NewDataFromStruct(stats)
	outputChan <- output
}

func (s *numberStats) Finish(outputChan chan ratchet.Data, killChan chan error) {
	close(outputChan)
}

func (s *numberStats) String() string {
	return "numberStats"
}

func main() {
	// Simple starter that sends a numbers array (defined in this example file).
	numberStarter := &numberStarter{}
	// Calculate some basic stats on the numbers (defined in this example file).
	numberStats := &numberStats{}
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
