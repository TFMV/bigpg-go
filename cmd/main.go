package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/TFMV/bigpg-go/pkg/bigquery"
	"github.com/TFMV/bigpg-go/pkg/postgres"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	ctx := context.Background()

	// Define Arrow Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	// Load BigQuery Service Account JSON
	sa, err := os.ReadFile("/Users/thomasmcgeehan/bigpg-go/bigpg-go/sa.json")
	if err != nil {
		log.Fatalf("Failed to read SA file: %v", err)
	}

	// Initialize PostgreSQL Source
	postgresSource, err := postgres.NewPostgresSource(ctx, "postgres://postgres:postgres@localhost:5432/foo")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer postgresSource.Close()

	// Initialize BigQuery Client
	bigqueryClient, err := bigquery.NewBigQueryWriteClient(ctx, string(sa), schema)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Create BigQuery Record Writer
	bqWriter, err := bigquery.NewBigQueryRecordWriter(
		ctx,
		bigqueryClient,
		"tfmv-371720", // Project ID
		"tfmv",        // Dataset ID
		"records",     // Table ID
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create BigQuery record writer: %v", err)
	}
	defer bqWriter.Close() // ❌ DON'T CLOSE IN THE LOOP

	// Read Records from PostgreSQL
	pgReader, err := postgresSource.GetPostgresRecordReader(ctx, "records")
	if err != nil {
		log.Fatalf("Failed to get PostgreSQL reader: %v", err)
	}
	defer pgReader.Close()

	// Write Each Record to BigQuery
	for {
		record, err := pgReader.Read()
		if err != nil {
			if err == io.EOF {
				break // No more records
			}
			log.Fatalf("Failed to read from PostgreSQL: %v", err)
		}

		// Write the record to BigQuery
		err = bqWriter.Write(record)
		if err != nil {
			log.Fatalf("Failed to write to BigQuery: %v", err)
		}

		record.Release() // Release Arrow memory
		fmt.Println("Successfully wrote record to BigQuery")
	}

	// **🚀 FINALIZE THE WRITE STREAM AFTER ALL RECORDS ARE WRITTEN**
	err = bqWriter.Close()
	if err != nil {
		log.Fatalf("Failed to finalize BigQuery write stream: %v", err)
	}

	fmt.Println("Data migration completed successfully.")
}
