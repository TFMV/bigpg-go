package postgres

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func (p *PostgresSource) GetPostgresRecordReader(ctx context.Context, tableName string) (*PostgresRecordReader, error) {
	stmt, err := p.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}

	// Force PostgreSQL to return the correct Arrow types
	query := fmt.Sprintf(`
		SELECT 
			dep_delay::BIGINT AS dep_delay, 
			arr_delay::BIGINT AS arr_delay, 
			air_time::BIGINT AS air_time, 
			distance::BIGINT AS distance, 
			dep_time::FLOAT8 AS dep_time, 
			arr_time::FLOAT8 AS arr_time 
		FROM %s`, tableName)

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

func ConvertNumericFields(record arrow.Record) arrow.Record {
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		switch record.ColumnName(i) {
		case "dep_time", "arr_time":
			strArray, ok := col.(*array.String)
			if !ok {
				continue
			}

			floatBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
			for j := 0; j < int(strArray.Len()); j++ {
				if strArray.IsNull(j) {
					floatBuilder.AppendNull()
				} else {
					val, err := strconv.ParseFloat(strArray.Value(j), 64)
					if err != nil {
						log.Printf("Warning: failed to convert %s to float64: %v", strArray.Value(j), err)
						floatBuilder.AppendNull()
					} else {
						floatBuilder.Append(val)
					}
				}
			}

			newColumn := floatBuilder.NewArray()
			record.SetColumn(i, newColumn)
		}
	}
	return record
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

func ConvertDateFields(record arrow.Record) arrow.Record {
	builder := array.NewDate64Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		if record.ColumnName(i) == "fl_date" {
			dateCol, ok := col.(*array.Date32)
			if !ok {
				continue
			}

			for j := 0; j < int(dateCol.Len()); j++ {
				if dateCol.IsNull(j) {
					builder.AppendNull()
				} else {
					// Convert days to milliseconds
					builder.Append(arrow.Date64(int64(dateCol.Value(j)) * 86400000))
				}
			}

			newColumn := builder.NewArray()
			record.SetColumn(i, newColumn)
		}
	}

	return record
}
