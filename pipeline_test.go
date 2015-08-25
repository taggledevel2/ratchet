package ratchet_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/DailyBurn/ratchet"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/processors"
)

func ExampleNewPipeline() {
	logger.LogLevel = logger.LevelSilent

	// A basic pipeline is created using one or more DataProcessor instances.
	hello := processors.NewIoReader(strings.NewReader("Hello world!"))
	stdout := processors.NewIoWriter(os.Stdout)
	stdout.AddNewline = true
	pipeline := ratchet.NewPipeline(hello, stdout)

	err := <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
	} else {
		fmt.Println("Ratchet pipeline ran successfully.")
	}

	// Output:
	// Hello world!
	// Ratchet pipeline ran successfully.
}

// func ExampleNewBranchingPipeline() {
// 	// A branched pipeline is created using slices of See ConcurrentDataProcessor instances.
// 	hello := stages.NewIoReader(strings.NewReader("Hello world"))
// 	hola := stages.NewIoReader(strings.NewReader("Hola mundo"))
// 	bonjour := stages.NewIoReader(strings.NewReader("Bonjour monde"))
// 	stdout := stages.NewIoWriter(os.Stdout)
// 	stdout.AddNewline = true
//
// 	stage1 := []ratchet.See ConcurrentDataProcessor{hello, hola, bonjour}
// 	stage2 := []ratchet.See ConcurrentDataProcessor{stdout}
// 	pipeline := ratchet.NewBranchingPipeline(stage1, stage2)
//
// 	err := <-pipeline.Run()
//
// 	if err != nil {
// 		fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
// 	} else {
// 		fmt.Println("Ratchet pipeline ran successfully.")
// 	}
// }
