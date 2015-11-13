package client

import bigquery "github.com/dailyburn/google-api-go-client-bigquery/bigquery/v2"

// InsertNewTable creates a new empty table for the given project and dataset with the field name/types defined in the fields map
func (c *Client) InsertNewTable(projectId, datasetId, tableName string, fields map[string]string) error {
	// If the table already exists, an error will be raised here.
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

func (c *Client) InsertNewTableIfDoesNotExist(projectId, datasetId, tableId string, fields map[string]string) error {
	// This will not return an error if the table already exists
	exists, err := c.tableDoesExist(projectId, datasetId, tableId)
	if err != nil {
		return err
	}
	if !exists {
		return c.InsertNewTable(projectId, datasetId, tableId, fields)
	}
	return nil
}

func (c *Client) tableDoesExist(projectId, datasetId, tableId string) (bool, error) {
	// return err only if connection fails
	service, err := c.connect()
	if err != nil {
		return false, err
	}

	_, err = service.Tables.Get(projectId, datasetId, tableId).Do()
	if err != nil {
		return false, nil
	}
	return true, nil
}
