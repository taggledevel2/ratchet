package processors

import (
	"database/sql"
	"errors"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
)

type SQLDeleter struct {
	readDB       *sql.DB
	query        string
	sqlGenerator func(data.JSON) (string, error)
}

// NewSQLDeleter returns a new SQLDeleter
func NewSQLDeleter(dbConn *sql.DB, sql string) *SQLDeleter {
	return &SQLDeleter{readDB: dbConn, query: sql}
}

// NewDynamicSQLDeleter returns a new SQLDeleter operating in dynamic mode.
func NewDynamicSQLDeleter(dbConn *sql.DB, sqlGenerator func(data.JSON) (string, error)) *SQLDeleter {
	return &SQLDeleter{readDB: dbConn, sqlGenerator: sqlGenerator}
}

func (s *SQLDeleter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	sql := ""
	var err error
	if s.query == "" && s.sqlGenerator != nil {
		sql, err = s.sqlGenerator(d)
		util.KillPipelineIfErr(err, killChan)
	} else if s.query != "" {
		sql = s.query
	} else {
		killChan <- errors.New("SQLDeleter: must have either static query or sqlGenerator func")
	}

	logger.Debug("SQLDeleter: Running - ", sql)
	// See sql.go
	err = util.DeleteDataViaSQLQuery(s.readDB, sql)
	util.KillPipelineIfErr(err, killChan)
}

// Finish - see interface for documentation.
func (s *SQLDeleter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLDeleter) String() string {
	return "SQLDeleter"
}
