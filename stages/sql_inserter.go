package stages

import (
	"database/sql"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// SQLInserter handles INSERTing data.JSON into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
//
// Note that the data.JSON must be a valid JSON object
// (or a slice of valid objects all with the same keys),
// where the keys are column names and the
// the values are SQL values to be inserted into those columns.
type SQLInserter struct {
	db             *sql.DB
	TableName      string
	OnDupKeyUpdate bool
}

// NewSQLInserter returns a new SQLInserter
func NewSQLInserter(db *sql.DB, tableName string) *SQLInserter {
	return &SQLInserter{db: db, TableName: tableName, OnDupKeyUpdate: true}
}

// ProcessData - see interface in stages.go for documentation.
func (s *SQLInserter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	err := util.SQLInsertData(s.db, d, s.TableName, s.OnDupKeyUpdate)
	util.KillPipelineIfErr(err, killChan)
}

// Finish - see interface for documentation.
func (s *SQLInserter) Finish(outputChan chan data.JSON, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (s *SQLInserter) String() string {
	return "SQLInserter"
}
