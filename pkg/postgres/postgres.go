package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// PostgresSource handles connection to a PostgreSQL database using ADBC.
type PostgresSource struct {
	conn adbc.Connection
}

// NewPostgresSource creates a new PostgresSource with an open ADBC connection.
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

// PostgresRecordReader reads records from a PostgreSQL table.
type PostgresRecordReader struct {
	ctx       context.Context
	stmt      adbc.Statement
	recordSet array.RecordReader
}

// GetPostgresRecordReader creates a PostgresRecordReader for the specified table.
func (p *PostgresSource) GetPostgresRecordReader(ctx context.Context, tableName string) (*PostgresRecordReader, error) {
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if err := stmt.SetSqlQuery(query); err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	recordSet, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &PostgresRecordReader{
		ctx:       ctx,
		stmt:      stmt,
		recordSet: recordSet,
	}, nil
}

// Read reads the next record from the PostgreSQL table.
func (r *PostgresRecordReader) Read() (arrow.Record, error) {
	if !r.recordSet.Next() {
		if err := r.recordSet.Err(); err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		return nil, io.EOF
	}

	record := r.recordSet.Record()
	record.Retain()
	return record, nil
}

// Schema returns the schema of the records being read.
func (r *PostgresRecordReader) Schema() *arrow.Schema {
	return r.recordSet.Schema()
}

// Close releases resources associated with the PostgresRecordReader.
func (r *PostgresRecordReader) Close() error {
	r.recordSet.Release()
	return r.stmt.Close()
}

// Close closes the ADBC connection associated with PostgresSource.
func (p *PostgresSource) Close() error {
	return p.conn.Close()
}

// PostgresSink handles writing records to a PostgreSQL database using ADBC.
type PostgresSink struct {
	conn adbc.Connection
}

// NewPostgresSink creates a new PostgresSink with an open ADBC connection.
func NewPostgresSink(ctx context.Context, dbURL string) (*PostgresSink, error) {
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

	return &PostgresSink{conn: conn}, nil
}

// IngestToPostgres ingests records from an arrow.Record into the specified PostgreSQL table.
func (p *PostgresSink) IngestToPostgres(ctx context.Context, tableName string, schema *arrow.Schema, record arrow.Record) error {
	// Construct the SQL query based on the schema
	columns := make([]string, len(schema.Fields()))
	values := make([]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columns[i] = field.Name
		values[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(values, ", "))

	// Prepare the statement
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return fmt.Errorf("failed to set SQL query: %w", err)
	}

	// Wrap the record in a SingleRecordReader to implement the array.RecordReader interface
	recordReader := NewSingleRecordReader(record)

	// Bind the record set as a stream
	if err := stmt.BindStream(ctx, recordReader); err != nil {
		record.Release()
		return fmt.Errorf("failed to bind stream: %w", err)
	}

	// Execute the insert statement
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		record.Release()
		return fmt.Errorf("failed to execute update: %w", err)
	}

	record.Release()
	return nil
}

// Close closes the ADBC connection associated with PostgresSink.
func (p *PostgresSink) Close() error {
	return p.conn.Close()
}
