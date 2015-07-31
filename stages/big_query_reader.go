package stages

import (
	"errors"

	bigquery "github.com/DailyBurn/bigquery/client"
	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// BigQueryReader is used to query data from Google's BigQuery,
// and it behaves similarly to SQLReader. See SQLReader
// docs for explanation on static vs dynamic querying.
type BigQueryReader struct {
	client       *bigquery.Client
	config       *BigQueryConfig
	query        string
	sqlGenerator func(data.JSON) (string, error)
	BatchSize    int
}

// BigQueryConfig is used when init'ing new BigQueryReader instances.
type BigQueryConfig struct {
	PemPath             string
	Secret              string
	AccountEmailAddress string
	AccountClientID     string
	ProjectID           string
	DatasetID           string
}

// NewBigQueryReader returns an instance of a BigQueryExtractor ready to
// run a static query.
func NewBigQueryReader(config *BigQueryConfig, query string) *BigQueryReader {
	r := BigQueryReader{}
	r.client = bigquery.New(config.PemPath, config.AccountEmailAddress, config.AccountClientID, config.Secret)
	r.query = query
	r.BatchSize = 2500 // default batch size
	return &r
}

// NewDynamicBigQueryReader returns an instance of a BigQueryExtractor ready to
// run a dynamic query based on the sqlGenerator function.
func NewDynamicBigQueryReader(config *BigQueryConfig, sqlGenerator func(data.JSON) (string, error)) *BigQueryReader {
	r := NewBigQueryReader(config, "")
	r.sqlGenerator = sqlGenerator
	return r
}

func (r *BigQueryReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	r.ForEachQueryData(d, killChan, func(d data.JSON) {
		outputChan <- d
	})
}

func (r *BigQueryReader) Finish(outputChan chan data.JSON, killChan chan error) {
	close(outputChan)
}

// ForEachQueryData handles generating the SQL (in case of dynamic mode),
// running the query and retrieving the data in data.JSON format, and then
// passing the results back witih the function call to forEach.
func (r *BigQueryReader) ForEachQueryData(d data.JSON, killChan chan error, forEach func(d data.JSON)) {
	sql := ""
	var err error
	if r.query == "" && r.sqlGenerator != nil {
		sql, err = r.sqlGenerator(d)
		util.KillPipelineIfErr(err, killChan)
	} else if r.query != "" {
		sql = r.query
	} else {
		killChan <- errors.New("BigQueryReader: must have either static query or sqlGenerator func")
	}

	bqDataChan := make(chan bigquery.Data)
	go r.client.AsyncQuery(r.BatchSize, r.config.DatasetID, r.config.ProjectID, sql, bqDataChan)

L:
	for {
		select {
		case bqData, ok := <-bqDataChan:
			util.KillPipelineIfErr(bqData.Err, killChan)
			logger.Debug("BigQueryReader: received data")

			if bqData.Rows != nil && bqData.Headers != nil {
				d := data.JSONFromHeaderAndRows(bqData.Headers, bqData.Rows)
				forEach(d) // pass back out via the forEach func
			}

			if !ok {
				logger.Info("BigQueryReader: done loading rows")
				break L
			}
		}
	}
}
