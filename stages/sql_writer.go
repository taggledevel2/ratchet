package stages

import (
	"database/sql"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/util"
)

// SQLWriter handles INSERTing data.JSON into a
// specified SQL table. If an error occurs while building
// or executing the INSERT, the error will be sent to the killChan.
//
//
// Note that the data.JSON must be a valid JSON object
// (or a slice of valid objects all with the same keys),
// where the keys are column names and the
// the values are SQL values to be inserted into those columns.
//
// For use-cases where a SQLWriter instance needs to write to
// multiple tables (e.g., when the preceding stage is building 2+
// separate data sets), you can pass in SQLWriterData.
type SQLWriter struct {
	writeDB        *sql.DB
	TableName      string
	OnDupKeyUpdate bool
}

// SQLWriterData is a custom data structure you can send into a SQLWriter
// stage if you need to specify TableName on a per-data payload basis. No
// extra configuration is needed to use SQLWriterData, each data payload
// received is first checked for this structure before processing.
type SQLWriterData struct {
	TableName  string
	InsertData data.JSON
}

// NewSQLWriter returns a new SQLWriter
func NewSQLWriter(db *sql.DB, tableName string) *SQLWriter {
	return &SQLWriter{writeDB: db, TableName: tableName, OnDupKeyUpdate: true}
}

func (s *SQLWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// First check for SQLWriterData
	wd := SQLWriterData{}
	err := data.ParseJSONSilent(d, &wd)
	if err != nil {
		// Normal data scenario
		err = util.SQLInsertData(s.writeDB, d, s.TableName, s.OnDupKeyUpdate)
		util.KillPipelineIfErr(err, killChan)
	} else {
		// SQLWriterData scenario
		err = util.SQLInsertData(s.writeDB, wd.InsertData, wd.TableName, s.OnDupKeyUpdate)
		util.KillPipelineIfErr(err, killChan)
	}
}

func (s *SQLWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	if outputChan != nil {
		close(outputChan)
	}
}

func (s *SQLWriter) String() string {
	return "SQLWriter"
}
