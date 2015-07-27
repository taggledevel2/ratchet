package starters

import (
	"database/sql"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// SQLQueryer is a starter that runs the given SQL and passes the
// resulting Data along to the next stage.
type SQLQueryer struct {
	db        *sql.DB
	query     string
	BatchSize int
}

// NewSQLQueryer returns a new SQLQueryer PipelineStarter.
func NewSQLQueryer(dbConn *sql.DB, sql string) *SQLQueryer {
	return &SQLQueryer{db: dbConn, query: sql, BatchSize: 100}
}

// Start - see interface in stages.go for documentation.
func (s *SQLQueryer) Start(outputChan chan data.JSON, killChan chan error) {
	// See sql.go
	dataChan, err := util.GetDataFromSQLQuery(s.db, s.query, s.BatchSize)
	util.KillPipelineIfErr(err, killChan)

	for d := range dataChan {
		outputChan <- d
	}
	close(outputChan)
}

func (s *SQLQueryer) String() string {
	return "SQLQueryer"
}
