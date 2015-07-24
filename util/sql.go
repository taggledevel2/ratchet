package util

import (
	"database/sql"

	"github.com/DailyBurn/ratchet"
)

// GetDataFromSQLQuery is a util function that, given a properly intialized sql.DB
// and a valid SQL query, will handle executing the query and getting back ratchet.Data
// objects. This function is asynch, and Data should be received on teh return
// data channel. If there was a problem setting up the query, then an error will also be
// returned immediately. It is also possible for errors to occur during execution as data
// is retrieved from the query. If this happens, the Data object returned will be a JSON
// object in the form of {"Error": "description"}.
func GetDataFromSQLQuery(db *sql.DB, query string, batchSize int) (chan ratchet.Data, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dataChan := make(chan ratchet.Data)

	go func(rows *sql.Rows, columns []string) {
		defer rows.Close()

		tableData := []map[string]interface{}{}
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for rows.Next() {
			for i := 0; i < len(columns); i++ {
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)
			entry := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
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
	}(rows, columns)

	return dataChan, nil
}

func sendTableData(tableData []map[string]interface{}, dataChan chan ratchet.Data) {
	data, err := ratchet.NewData(tableData)
	if err != nil {
		sendErr(err, dataChan)
	} else {
		dataChan <- data
	}
}

func sendErr(err error, dataChan chan ratchet.Data) {
	dataChan <- []byte("{\"Error\":\"" + err.Error() + "\"}")
}
