package processors_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/dailyburn/ratchet"
	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/processors"
)

func ExampleGetRequest() {
	logger.LogLevel = logger.LevelSilent

	getGoogle, err := processors.NewHTTPRequest("GET", "http://www.google.com", nil)
	if err != nil {
		panic(err)
	}
	// this is just a really basic checking function so we can have
	// determinable example output.
	checkHTML := processors.NewFuncTransformer(func(d data.JSON) data.JSON {
		output := "Got HTML?\n"
		if strings.Contains(strings.ToLower(string(d)), "html") {
			output += "YES\n"
		} else {
			output += "NO\n"
		}
		output += "HTML contains Google Search?\n"
		if strings.Contains(string(d), "Google Search") {
			output += "YES\n"
		} else {
			output += "NO\n"
		}
		return data.JSON(output)
	})
	stdout := processors.NewIoWriter(os.Stdout)
	pipeline := ratchet.NewPipeline(getGoogle, checkHTML, stdout)

	err = <-pipeline.Run()

	if err != nil {
		fmt.Println("An error occurred in the ratchet pipeline:", err.Error())
	}

	// Output:
	// Got HTML?
	// YES
	// HTML contains Google Search?
	// YES
}
