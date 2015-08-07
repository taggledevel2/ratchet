package util

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/DailyBurn/ratchet/data"
	"github.com/DailyBurn/ratchet/logger"
	"github.com/kisielk/sqlstruct"
)

// GetDataFromSQLQuery is a util function that, given a properly intialized sql.DB
// and a valid SQL query, will handle executing the query and getting back data.JSON
// objects. This function is asynch, and data.JSON should be received on the return
// data channel. If there was a problem setting up the query, then an error will also be
// returned immediately. It is also possible for errors to occur during execution as data
// is retrieved from the query. If this happens, the object returned will be a JSON
// object in the form of {"Error": "description"}.
func GetDataFromSQLQuery(db *sql.DB, query string, batchSize int, structDest interface{}) (chan data.JSON, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dataChan := make(chan data.JSON)

	if structDest != nil {
		go scanRowsUsingStruct(rows, columns, structDest, batchSize, dataChan)
	} else {
		go scanDataGeneric(rows, columns, batchSize, dataChan)
	}

	go func(rows *sql.Rows, columns []string) {

	}(rows, columns)

	return dataChan, nil
}

func scanRowsUsingStruct(rows *sql.Rows, columns []string, structDest interface{}, batchSize int, dataChan chan data.JSON) {
	defer rows.Close()

	tableData := []map[string]interface{}{}

	for rows.Next() {
		err := sqlstruct.Scan(structDest, rows)
		if err != nil {
			sendErr(err, dataChan)
		}

		d, err := data.NewJSON(structDest)
		if err != nil {
			sendErr(err, dataChan)
		}

		entry := make(map[string]interface{})
		err = data.ParseJSON(d, &entry)
		if err != nil {
			sendErr(err, dataChan)
		}

		tableData = append(tableData, entry)

		if batchSize > 0 && len(tableData) >= batchSize {
			sendTableData(tableData, dataChan)
			tableData = []map[string]interface{}{}
		}
	}
	if rows.Err() != nil {
		sendErr(rows.Err(), dataChan)
	}

	// Flush remaining tableData
	if len(tableData) > 0 {
		sendTableData(tableData, dataChan)
	}

	close(dataChan) // signal completion to caller
}

func scanDataGeneric(rows *sql.Rows, columns []string, batchSize int, dataChan chan data.JSON) {
	defer rows.Close()

	tableData := []map[string]interface{}{}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			sendErr(err, dataChan)
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			logger.Debug("Value Type for", col, " -> ", reflect.TypeOf(val))
			switch vv := val.(type) {
			case []byte:
				v = string(vv)
			default:
				v = vv
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)

		if batchSize > 0 && len(tableData) >= batchSize {
			sendTableData(tableData, dataChan)
			tableData = []map[string]interface{}{}
		}
	}
	if rows.Err() != nil {
		sendErr(rows.Err(), dataChan)
	}

	// Flush remaining tableData
	if len(tableData) > 0 {
		sendTableData(tableData, dataChan)
	}

	close(dataChan) // signal completion to caller
}

// http://play.golang.org/p/2wHfO6YS3_
func determineBytesValue(b []byte) (interface{}, error) {
	var v interface{}
	err := data.ParseJSONSilent(b, &v)
	if err != nil {
		// need to quote strings for JSON to parse correctly
		if !strings.Contains(string(b), `"`) {
			b = []byte(fmt.Sprintf(`"%v"`, string(b)))
			return determineBytesValue(b)
		}
	}
	switch vv := v.(type) {
	case []byte:
		return string(vv), err
	default:
		return v, err
	}
}

func sendTableData(tableData []map[string]interface{}, dataChan chan data.JSON) {
	d, err := data.NewJSON(tableData)
	if err != nil {
		sendErr(err, dataChan)
	} else {
		dataChan <- d
	}
}

func sendErr(err error, dataChan chan data.JSON) {
	dataChan <- []byte("{\"Error\":\"" + err.Error() + "\"}")
}

// SQLInsertData abstracts building and executing a SQL INSERT
// statement for the given Data object.
//
// Note that the Data must be a valid JSON object
// (or an array of valid objects all with the same keys),
// where the keys are column names and the
// the values are SQL values to be inserted into those columns.
func SQLInsertData(db *sql.DB, d data.JSON, tableName string, onDupKeyUpdate bool) error {
	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		return err
	}
	return insertObjects(db, objects, tableName, onDupKeyUpdate)
}

func insertObjects(db *sql.DB, objects []map[string]interface{}, tableName string, onDupKeyUpdate bool) error {
	insertSQL := buildInsertSQL(objects, tableName, onDupKeyUpdate)

	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return err
	}

	cols := sortedColumns(objects[0])
	vals := []interface{}{}
	for _, object := range objects {
		// Since maps aren't ordered, must iterate over sorted columns
		// for each object.
		for _, k := range cols {
			vals = append(vals, object[k])
		}
	}

	res, err := stmt.Exec(vals...)
	if err != nil {
		return err
	}
	lastID, err := res.LastInsertId()
	if err != nil {
		return err
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("SQLInsertData: rows affected = %d, last insert ID = %d", rowCnt, lastID))
	return nil
}

func buildInsertSQL(objects []map[string]interface{}, tableName string, onDupKeyUpdate bool) (insertSQL string) {
	cols := sortedColumns(objects[0])

	// Format: INSERT INTO tablename(col1,col2) VALUES(?,?),(?,?)
	insertSQL = fmt.Sprintf("INSERT INTO %v(%v) VALUES", tableName, strings.Join(cols, ","))
	// builds the (?,?) part
	vals := "("
	for i := 0; i < len(cols); i++ {
		if i > 0 {
			vals += ","
		}
		vals += "?"
	}
	vals += ")"
	// append as many (?,?) parts as there are objects to insert
	for i := 0; i < len(objects); i++ {
		if i > 0 {
			insertSQL += ","
		}
		insertSQL += vals
	}

	if onDupKeyUpdate {
		// format: ON DUPLICATE KEY UPDATE a=VALUES(a), b=VALUES(b), c=VALUES(c)
		insertSQL += " ON DUPLICATE KEY UPDATE "
		for i, c := range cols {
			if i > 0 {
				insertSQL += ","
			}
			insertSQL += "`" + c + "`=VALUES(`" + c + "`)"
		}
	}

	return insertSQL
}

func sortedColumns(object map[string]interface{}) []string {
	cols := []string{}
	for k := range object {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}
