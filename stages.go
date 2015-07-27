package ratchet

import (
	"github.com/DailyBurn/ratchet/data"
)

// PipelineStage is the interface used to process data at a certain step
// within a Pipeline. The Pipeline is responsible for passing data between
// each stage, and each stage will receive data in it's HandleData function.
//
// When the stage is finished handling it's current data payload, it should send
// the new Data on the outputChan to the next stage.
//
// When the stage is finished sending all of it's data, it MUST close(outputChan).
// This will almost always occur in the stage's Finish() function, which is called
// after the previous stage has closed it's ouput channel (i.e. is finished sending data).
// Finish() can also be used to send a final Data payload in use-cases where the stage
// is batching up multiple Data payloads and needs to send an aggregated Data object
// once all processing is complete.
//
// If an unexpected error occurs, it should be sent to the killChan to halt
// pipeline execution.
type PipelineStage interface {
	// HandleData will be called for each data sent on the previous stage's outputChan
	HandleData(d data.JSON, outputChan chan data.JSON, killChan chan error)

	// Finish will be called after the previous stage has closed it's outputChan
	// and won't be sending any more data. So, Finish() will be be called after
	// the last call to HandleData().
	//
	// *Note: If the PipelineStage instance receiving the Finish() call is the last
	// stage in the pipeline, outputChan will be nil.*
	Finish(outputChan chan data.JSON, killChan chan error)
}

// PipelineStarter is responsible for kicking off the pipeline
// and sending the initial Data objects to outputChan. When PipelineStarter
// is finished pulling and sending data, it MUST close(outputChan).
type PipelineStarter interface {
	Start(outputChan chan data.JSON, killChan chan error)
}
