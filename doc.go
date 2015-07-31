/*
Ratchet is a library for performing data pipeline / ETL tasks in Go.

The main construct in Ratchet is Pipeline. A Pipeline can have many
PipelineStage's, which will each perform some type of data processing, and
then send new data on to the next stage. Each PipelineStage will run in
it's own goroutine, making it simple to process data in different stages
concurrently (all the details are handled in the background).

Here is a conceptual drawing of a fairly simple Pipeline:

        +--Pipeline------------------------------------------------------------------------------------------+
        |                                                                       Stage 3                      |
        |                                                                      +---------------------------+ |
        |  Stage 1                        Stage 2                   +-JSON---> |  CSVWriter                | |
        | +------------------+           +-----------------------+  |          +---------------------------+ |
        | |  SQLReader       +-JSON----> | Custom PipelineStage  +--+                                        |
        | +------------------+           +-----------------------+  |          +---------------------------+ |
        |                                                           +-JSON---> |  SQLWriter                | |
        |                                                                      +---------------------------+ |
        +----------------------------------------------------------------------------------------------------+

In this example, we have a Pipeline consisting of 3 stages. The first stage is
pulling data from a SQL database, the second stage is doing custom transformation
processing on that data, and the third stage branches into 2 stages, one
writing the resulting data to a CSV file, and the other inserting into another
SQL database.

Since each stage is running in it's own goroutine, SQLReader can continue pulling and sending
data while each subsequent stage is also processing data. Optimally-designed pipelines
have stages that can each run in an isolated fashion, processing data without having
to worry about what's coming next down the pipeline.

In the example above, Stage 1 and Stage 3 are using built-in PipelineStage
implementations (see the "stages" package/subdirectory). However, Stage 2 is using a custom
implementation of PipelineStage. By using a combination of built-in generic
stages, and supporting the writing any Go code to process data, Ratchet makes
it possible to write very custom and fast data pipeline systems. See the
documentation for the PipelineStage interface for details on implementation
custom stages.

All data payloads sent between stages are of type JSON. By convention,
Ratchet expects valid JSON to be what is sent on data channels. It's worth
noting that JSON is actually represented as []byte though, and it is sometimes
useful to ignore the stipulation for valid JSON if your pipeline will support it.

Note: Many of the concepts in Ratchet were taken from the Golang blog's post on
pipelines (http://blog.golang.org/pipelines).
*/
package ratchet
