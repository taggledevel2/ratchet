package ratchet_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/DailyBurn/ratchet"
	"github.com/DailyBurn/ratchet/stages"
)

func ExampleNewPipeline() {
	// A basic pipeline is created using one or more PipelineStage instances.
	hello := stages.NewIoReader(strings.NewReader("Hello world!"))
	stdout := stages.NewIoWriter(os.Stdout)
	pipeline := ratchet.NewPipeline(hello, stdout)

	err := <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
	} else {
		fmt.Println("Ratchet pipeline ran successfully.")
	}
}

func ExampleNewBranchingPipeline() {
	// A branched pipeline is created using slices of PipelineStage instances.
	hello := stages.NewIoReader(strings.NewReader("Hello world"))
	hola := stages.NewIoReader(strings.NewReader("Hola mundo"))
	bonjour := stages.NewIoReader(strings.NewReader("Bonjour monde"))
	stdout := stages.NewIoWriter(os.Stdout)
	stdout.AddNewline = true

	stage1 := []ratchet.PipelineStage{hello, hola, bonjour}
	stage2 := []ratchet.PipelineStage{stdout}
	pipeline := ratchet.NewBranchingPipeline(stage1, stage2)

	err := <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
	} else {
		fmt.Println("Ratchet pipeline ran successfully.")
	}
}
