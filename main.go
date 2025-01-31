package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type PostgresSource struct {
	conn adbc.Connection
}

// NewPostgresSource initializes a new PostgresSource with an open ADBC connection.
func NewPostgresSource(ctx context.Context, dbURL string) (*PostgresSource, error) {
	drv := drivermgr.Driver{}
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: dbURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	return &PostgresSource{conn: conn}, nil
}

// ScanTable executes a query and returns a RecordReader for the specified table.
func (p *PostgresSource) ScanTable(ctx context.Context, tableName string) (array.RecordReader, error) {
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return reader, nil
}

// Close releases resources associated with the PostgresSource.
func (p *PostgresSource) Close() error {
	return p.conn.Close()
}

// ArrowToJSON converts Arrow records into JSON format and writes them to the output writer.
func ArrowToJSON(ctx context.Context, reader array.RecordReader, output io.Writer) error {
	defer reader.Release()

	encoder := json.NewEncoder(output)

	for reader.Next() {
		record := reader.Record()
		fields := record.Schema().Fields()

		cols := make(map[string]interface{})
		for i := 0; int64(i) < record.NumRows(); i++ {
			for j, col := range record.Columns() {
				cols[fields[j].Name] = col.GetOneForMarshal(i)
			}
			if err := encoder.Encode(cols); err != nil {
				return fmt.Errorf("failed to encode JSON: %w", err)
			}
		}
	}

	// Handle errors encountered during reading
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading records: %w", err)
	}

	return nil
}
