package main

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/TFMV/bigpg-go/pkg/bigquery"
	"github.com/TFMV/bigpg-go/pkg/postgres"
	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap"
)

const (
	batchSize      = 50000 // Number of rows per batch
	maxParallelism = 5     // Number of parallel BigQuery writes
)

func main() {
	// Initialize zap logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx := context.Background()
	startTime := time.Now()

	// Define Arrow Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "dep_delay", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "arr_delay", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "air_time", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "distance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "dep_time", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "arr_time", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	// Load BigQuery Service Account JSON
	sa, err := os.ReadFile("../sa.json")
	if err != nil {
		logger.Fatal("Failed to read SA file", zap.Error(err))
	}

	// Initialize PostgreSQL Source
	postgresSource, err := postgres.NewPostgresSource(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		logger.Fatal("Failed to connect to PostgreSQL", zap.Error(err))
	}
	defer postgresSource.Close()

	// Initialize BigQuery Client
	bigqueryClient, err := bigquery.NewBigQueryWriteClient(ctx, string(sa), schema)
	if err != nil {
		logger.Fatal("Failed to create BigQuery client", zap.Error(err))
	}

	// Create BigQuery Record Writer
	bqWriter, err := bigquery.NewBigQueryRecordWriter(ctx, bigqueryClient, "tfmv-371720", "tfmv", "flights", nil)
	if err != nil {
		logger.Fatal("Failed to create BigQuery record writer", zap.Error(err))
	}
	defer bqWriter.Close()

	// Read Records from PostgreSQL
	pgReader, err := postgresSource.GetPostgresRecordReader(ctx, "flights")
	if err != nil {
		logger.Fatal("Failed to get PostgreSQL reader", zap.Error(err))
	}
	defer pgReader.Close()

	// Worker pool for parallel writes
	batchCh := make(chan []arrow.Record, maxParallelism)
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < maxParallelism; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for batch := range batchCh {
				err := sendBatchToBigQuery(bqWriter, batch, workerID)
				if err != nil {
					logger.Error("Failed to write batch to BigQuery", zap.Error(err))
				}
			}
		}(i)
	}

	// Track metrics
	var rowCount int64
	var batch []arrow.Record
	batchStartTime := time.Now()

	// Read and send records in batches
	for {
		record, err := pgReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Fatal("Failed to read from PostgreSQL", zap.Error(err))
		}

		record = postgres.ConvertNumericFields(record)
		batch = append(batch, record)
		rowCount += record.NumRows()

		// Send batch when it reaches batchSize
		if rowCount >= batchSize {
			batchCh <- batch // Send batch to worker
			batch = nil      // Reset batch
			rowCount = 0     // Reset row count

			// Log batch performance
			batchDuration := time.Since(batchStartTime)
			logger.Info("Batch sent",
				zap.Int("batch_size", batchSize),
				zap.Duration("batch_duration", batchDuration),
				zap.Float64("rows_per_second", float64(batchSize)/batchDuration.Seconds()),
			)
			batchStartTime = time.Now()
		}
	}

	// Send any remaining records
	if len(batch) > 0 {
		batchCh <- batch
	}

	close(batchCh) // Close channel when done sending batches
	wg.Wait()      // Wait for all writes to finish

	// Finalize write stream
	err = bqWriter.Close()
	if err != nil {
		logger.Fatal("Failed to finalize BigQuery write stream", zap.Error(err))
	}

	// Log final metrics
	totalDuration := time.Since(startTime)
	logger.Info("Data migration completed",
		zap.Duration("total_duration", totalDuration),
	)
}

// Function to send a batch of records to BigQuery
func sendBatchToBigQuery(bqWriter *bigquery.BigQueryRecordWriter, batch []arrow.Record, workerID int) error {
	totalRows := int64(0)

	for _, record := range batch {
		totalRows += record.NumRows()
		err := bqWriter.Write(record)
		if err != nil {
			return err
		}
		record.Release() // Free Arrow memory after writing
	}

	// Log batch completion
	zap.L().Info("Batch sent to BigQuery",
		zap.Int("worker_id", workerID),
		zap.Int64("rows_written", totalRows),
	)
	return nil
}
