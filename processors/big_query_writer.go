package processors

import (
	bigquery "github.com/dailyburn/bigquery/client"
	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
)

type BigQueryWriter struct {
	client            *bigquery.Client
	config            *BigQueryConfig
	tableName         string
	fieldsForNewTable map[string]string
	ConcurrencyLevel  int // See ConcurrentDataProcessor
}

func NewBigQueryWriter(config *BigQueryConfig, tableName string) *BigQueryWriter {
	w := BigQueryWriter{config: config, tableName: tableName}
	return &w
}

func NewBigQueryWriterForNewTable(config *BigQueryConfig, tableName string, fields map[string]string) *BigQueryWriter {
	// This writer will attempt to write new table with the provided fields if it does not already exist.
	w := BigQueryWriter{config: config, tableName: tableName, fieldsForNewTable: fields}
	return &w
}

func (w *BigQueryWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	queuedRows, err := data.ObjectsFromJSON(d)
	util.KillPipelineIfErr(err, killChan)

	logger.Info("BigQueryWriter: Writing -", len(queuedRows))
	err = w.WriteBatch(queuedRows)
	if err != nil {
		util.KillPipelineIfErr(err, killChan)
	}
	logger.Info("BigQueryWriter: Write complete")
}

func (w *BigQueryWriter) WriteBatch(queuedRows []map[string]interface{}) (err error) {
	err = w.bqClient().InsertRows(w.config.ProjectID, w.config.DatasetID, w.tableName, queuedRows)
	return err
}

func (w *BigQueryWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *BigQueryWriter) String() string {
	return "BigQueryWriter"
}

// See ConcurrentDataProcessor
func (w *BigQueryWriter) Concurrency() int {
	return w.ConcurrencyLevel
}

func (w *BigQueryWriter) bqClient() *bigquery.Client {
	if w.client == nil {
		w.client = bigquery.New(w.config.JsonPemPath)
		w.client.PrintDebug = true
		if w.fieldsForNewTable != nil {
			err := w.client.InsertNewTableIfDoesNotExist(w.config.ProjectID, w.config.DatasetID, w.tableName, w.fieldsForNewTable)
			if err != nil {
				// Only thrown if table existence could not be verified or if the table could not be created.
				panic(err)
			}
		}
	}
	return w.client
}
