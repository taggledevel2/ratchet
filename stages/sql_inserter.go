package stages

import (
	"database/sql"

	"github.com/DailyBurn/ratchet"
)

// SQLInserter handles INSERTing ratchet.Data into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
//
// Note that the ratchet.Data must be a valid JSON object
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

// HandleData - see interface in stages.go for documentation.
func (s *SQLInserter) HandleData(data ratchet.Data, outputChan chan ratchet.Data, killChan chan error) {
	err := ratchet.SQLInsertData(s.db, data, s.TableName, s.OnDupKeyUpdate)
	if err != nil {
		killChan <- err
	}
}

// Finish - see interface in stages.go for documentation.
func (s *SQLInserter) Finish(outputChan chan ratchet.Data, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (s *SQLInserter) String() string {
	return "SQLInserter"
}
