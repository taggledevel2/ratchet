package ratchet

// PipelineStage is the interface used to process data at a certain step
// within a Pipeline. The Pipeline is responsible for passing data between
// each stage, and each stage will receive data in it's HandleData function.
//
// When the stage is finished handling it's current data payload, it should send
// the new Data on the outputChan to the next stage.
//
// If an unexpected error occurs, it can be sent to the exitChan to halt
// pipeline execution.
type PipelineStage interface {
	// HandleData will be called for each data sent on the previous stage's outputChan
	HandleData(data Data, outputChan chan Data, exitChan chan error)

	// Finish will be called after the previous stage has closed it's outputChan
	// and won't be sending any more data. So, Finish() will be be called after
	// the last call to HandleData().
	//
	// *Note: If the PipelineStage instance receiving the Finish() call is the last
	// stage in the pipeline, outputChan will be nil.*
	Finish(outputChan chan Data, exitChan chan error)
}

// PipelineStarter is responsible for kicking off the initial stage of
// the pipeline and sending the initial Data objects to outputChan.
type PipelineStarter interface {
	Start(outputChan chan Data, exitChan chan error)
}
