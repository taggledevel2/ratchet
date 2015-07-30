package stages

import (
	"database/sql"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// SQLReaderWriter performs both the job of a SQLReader and SQLWriter.
// This means it will run a SQL query, write the resulting data into a
// SQL database, as well as send the queried data to the next stage
// of processing.
//
// SQLReaderWriter is composed of both a SQLReader and SQLWriter, so it
// supports all of the same properties and usage options (such as static
// versus dynamic SQL querying).
type SQLReaderWriter struct {
	SQLReader
	SQLWriter
}

// NewSQLReaderWriter returns a new SQLReaderWriter ready for static querying.
func NewSQLReaderWriter(readConn *sql.DB, writeConn *sql.DB, readQuery, writeTable string) *SQLReaderWriter {
	s := SQLReaderWriter{}
	s.readDB = readConn
	s.writeDB = writeConn
	s.query = readQuery
	s.TableName = writeTable
	return &s
}

// NewSQLDynamicReaderWriter returns a new SQLReaderWriter ready for dynamic querying.
func NewSQLDynamicReaderWriter(readConn *sql.DB, writeConn *sql.DB, sqlGenerator func(data.JSON) string, writeTable string) *SQLReaderWriter {
	s := NewSQLReaderWriter(readConn, writeConn, "", writeTable)
	s.sqlGenerator = sqlGenerator
	return s
}

func (s *SQLReaderWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// Using SQLReader methods for processing data - this works via composition.
	s.ForEachQueryData(d, killChan, func(d data.JSON) {
		err := util.SQLInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate)
		util.KillPipelineIfErr(err, killChan)

		outputChan <- d
	})
}

func (s *SQLReaderWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	close(outputChan)
}

func (s *SQLReaderWriter) String() string {
	return "SQLReaderWriter"
}
