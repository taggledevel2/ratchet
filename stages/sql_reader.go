package stages

import (
	"database/sql"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/DailyBurn/ratchet/util"
)

// SQLReader is a starter that runs the given SQL and passes the
// resulting Data along to the next stage.
type SQLReader struct {
	db        *sql.DB
	query     string
	BatchSize int
}

// NewSQLReader returns a new SQLReader PipelineStarter.
func NewSQLReader(dbConn *sql.DB, sql string) *SQLReader {
	return &SQLReader{db: dbConn, query: sql, BatchSize: 100}
}

// ProcessData - see interface for documentation.
func (s *SQLReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	logger.Debug("SQLReader: Running - ", s.query)
	// See sql.go
	dataChan, err := util.GetDataFromSQLQuery(s.db, s.query, s.BatchSize)
	util.KillPipelineIfErr(err, killChan)

	for d := range dataChan {
		outputChan <- d
	}
}

// Finish - see interface for documentation.
func (s *SQLReader) Finish(outputChan chan data.JSON, killChan chan error) {
	close(outputChan)
}

func (s *SQLReader) String() string {
	return "SQLReader"
}
