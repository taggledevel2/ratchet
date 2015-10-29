package client

import bigquery "github.com/Dailyburn/google-api-go-client-bigquery/bigquery/v2"

// STRING
// INTEGER
// FLOAT
// BOOLEAN
// TIMESTAMP
// RECORD

// creates a new empty table for the given project and dataset with the field name/types defined in the fields map
func (c *Client) InsertNewTable(tableName, projectId, datasetId string, fields map[string]string) error {
	service, err := c.connect()
	if err != nil {
		return err
	}

	// build the table schema
	schema := &bigquery.TableSchema{}
	for k, v := range fields {
		schema.Fields = append(schema.Fields, &bigquery.TableFieldSchema{Name: k, Type: v})
	}

	// build the table to insert
	table := &bigquery.Table{}
	table.Schema = schema

	tr := &bigquery.TableReference{}
	tr.DatasetId = datasetId
	tr.ProjectId = projectId
	tr.TableId = tableName

	table.TableReference = tr

	_, err = service.Tables.Insert(projectId, datasetId, table).Do()
	if err != nil {
		return err
	}

	return nil
}
