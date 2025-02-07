package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"

	"io"
	"time"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	arrowpb "github.com/TFMV/bigpg-go/arrowpb"
	memoryPool "github.com/TFMV/bigpg-go/internal/memory"
	arrowproto "github.com/TFMV/bigpg-go/proto"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

const maxParallelism = 5 // Number of concurrent workers

type BigQueryWriteClient struct {
	client *storage.BigQueryWriteClient
	schema *arrow.Schema
}

type BigQueryWriteOptions struct {
	WriteStreamType storagepb.WriteStream_Type
	Allocator       memory.Allocator
}

func NewDefaultBigQueryWriteOptions() *BigQueryWriteOptions {
	return &BigQueryWriteOptions{
		WriteStreamType: storagepb.WriteStream_COMMITTED,
		Allocator:       memoryPool.GetAllocator(),
	}
}

func NewBigQueryWriteClient(ctx context.Context, serviceAccountJSON string, schema *arrow.Schema) (*BigQueryWriteClient, error) {
	// Check if the provided string is a file path
	if _, err := os.Stat(serviceAccountJSON); err == nil {
		content, err := os.ReadFile(serviceAccountJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to read service account JSON file: %w", err)
		}
		serviceAccountJSON = string(content)
	}

	client, err := storage.NewBigQueryWriteClient(ctx, option.WithCredentialsJSON([]byte(serviceAccountJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery Storage API client: %w", err)
	}

	// Fix Schema Mismatches
	fields := make([]arrow.Field, len(schema.Fields()))
	for i, field := range schema.Fields() {
		switch field.Name {
		case "fl_date":
			fields[i] = arrow.Field{
				Name:     field.Name,
				Type:     &arrow.Date64Type{}, // Force date64
				Nullable: field.Nullable,
			}
		case "dep_delay", "arr_delay", "air_time", "distance":
			fields[i] = arrow.Field{
				Name:     field.Name,
				Type:     arrow.PrimitiveTypes.Int64, // Ensure int64
				Nullable: field.Nullable,
			}
		case "dep_time", "arr_time":
			fields[i] = arrow.Field{
				Name:     field.Name,
				Type:     arrow.PrimitiveTypes.Float64, // Ensure float64
				Nullable: field.Nullable,
			}
		default:
			fields[i] = field
		}
	}

	// Apply fixed schema
	schemaWithFixes := arrow.NewSchema(fields, nil)

	return &BigQueryWriteClient{
		client: client,
		schema: schemaWithFixes,
	}, nil
}

type BigQueryRecordWriter struct {
	client        *BigQueryWriteClient
	writeStream   *storagepb.WriteStream
	protoSchema   *storagepb.ProtoSchema
	writerOptions *BigQueryWriteOptions

	appendClient storagepb.BigQueryWrite_AppendRowsClient
	bufferPool   sync.Pool
	wg           sync.WaitGroup
	workerCh     chan *arrow.Record
}

func NewBigQueryRecordWriter(ctx context.Context, client *BigQueryWriteClient, projectID, datasetID, tableID string, opts *BigQueryWriteOptions) (*BigQueryRecordWriter, error) {
	if opts == nil {
		opts = NewDefaultBigQueryWriteOptions()
	}

	tableName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID)

	writeStream, err := client.client.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: tableName,
		WriteStream: &storagepb.WriteStream{
			Type: opts.WriteStreamType,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create write stream: %w", err)
	}

	appendClient, err := client.client.AppendRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open AppendRows client: %w", err)
	}

	writer := &BigQueryRecordWriter{
		client:        client,
		appendClient:  appendClient,
		writeStream:   writeStream,
		protoSchema:   arrowpb.ArrowSchemaToProto(client.schema),
		writerOptions: opts,
		workerCh:      make(chan *arrow.Record, maxParallelism),
	}

	// Initialize worker pool
	for i := 0; i < maxParallelism; i++ {
		go writer.worker(ctx)
	}

	return writer, nil
}

func (w *BigQueryRecordWriter) worker(ctx context.Context) {
	for record := range w.workerCh {
		err := w.writeBatch(ctx, record)
		if err != nil {
			fmt.Printf("Error writing batch: %v\n", err)
		}
		w.wg.Done()
	}
}

func (w *BigQueryRecordWriter) writeBatch(ctx context.Context, record *arrow.Record) error {
	fmt.Printf("Writing record with %d rows and %d columns\n", (*record).NumRows(), (*record).NumCols())

	buffer := &bytes.Buffer{}
	ipcWriter := ipc.NewWriter(buffer, ipc.WithSchema(w.client.schema), ipc.WithAllocator(w.writerOptions.Allocator))
	defer ipcWriter.Close()

	if err := ipcWriter.Write(*record); err != nil {
		return fmt.Errorf("error writing record to buffer: %w", err)
	}

	serializedData := buffer.Bytes()
	if len(serializedData) == 0 {
		return fmt.Errorf("serialized data is empty")
	}

	protoData := &storagepb.AppendRowsRequest_ProtoData{
		Rows: &storagepb.ProtoRows{
			SerializedRows: [][]byte{serializedData},
		},
		WriterSchema: w.protoSchema,
	}

	appendReq := &storagepb.AppendRowsRequest{
		WriteStream: w.writeStream.GetName(),
		Rows:        &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: protoData},
	}

	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := w.appendClient.Send(appendReq)
			if err == nil {
				resp, err := w.appendClient.Recv()
				if err != nil {
					fmt.Printf("Error receiving response: %v\n", err)
				} else {
					fmt.Printf("Response received: %+v\n", resp)
				}
				return nil
			}

			fmt.Printf("Error sending AppendRowsRequest (attempt %d of %d): %v\n", attempt+1, maxRetries, err)

			if err == io.EOF {
				if err := w.recreateAppendClient(); err != nil {
					return fmt.Errorf("failed to recreate append client: %w", err)
				}
				continue
			}

			time.Sleep(time.Second * time.Duration(attempt+1))
		}
	}

	return fmt.Errorf("failed to send AppendRowsRequest after %d attempts", maxRetries)
}

func (w *BigQueryRecordWriter) recreateAppendClient() error {
	var err error
	ctx := context.Background()
	w.appendClient, err = w.client.client.AppendRows(ctx)
	return err
}

func (w *BigQueryRecordWriter) Write(record arrow.Record) error {
	fmt.Printf("Writing record with %d rows and %d columns\n", record.NumRows(), record.NumCols())

	if !w.client.schema.Equal(record.Schema()) {
		return fmt.Errorf("schema mismatch: expected %v but got %v", w.client.schema, record.Schema())
	}

	// Convert Arrow record to Protobuf rows
	rows, err := arrowpb.ConvertArrowRecord(record)
	if err != nil {
		return fmt.Errorf("failed to convert Arrow record to Protobuf: %w", err)
	}

	// Serialize all rows into a single batch
	serializedData, err := proto.Marshal(&arrowproto.Batch{
		Rows: rows,
	})
	if err != nil {
		return fmt.Errorf("failed to serialize batch: %w", err)
	}

	protoData := &storagepb.AppendRowsRequest_ProtoData{
		Rows: &storagepb.ProtoRows{
			SerializedRows: [][]byte{serializedData}, // Send as one large batch
		},
		WriterSchema: w.protoSchema,
	}

	appendReq := &storagepb.AppendRowsRequest{
		WriteStream: w.writeStream.GetName(),
		Rows:        &storagepb.AppendRowsRequest_ProtoRows{ProtoRows: protoData},
	}

	// Add logging for request details
	fmt.Printf("Sending AppendRowsRequest to stream: %s\n", w.writeStream.GetName())

	maxRetries := 3
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := w.appendClient.Send(appendReq)
		if err == nil {
			// Add logging for response
			resp, err := w.appendClient.Recv()
			if err != nil {
				fmt.Printf("Error receiving response: %v\n", err)
			} else {
				fmt.Printf("Response received: %+v\n", resp)
			}
			return nil
		}

		lastErr = err
		fmt.Printf("Error sending AppendRowsRequest (attempt %d of %d): %v\n", attempt+1, maxRetries, err)

		if err == io.EOF {
			if err := w.recreateAppendClient(); err != nil {
				return fmt.Errorf("failed to recreate append client: %w", err)
			}
			continue
		}

		time.Sleep(time.Second * time.Duration(attempt+1))
	}

	return fmt.Errorf("failed to send AppendRowsRequest after %d attempts: %w", maxRetries, lastErr)
}

func (w *BigQueryRecordWriter) Close() error {
	close(w.workerCh)
	w.wg.Wait() // Wait for all workers to finish

	finalizeRequest := &storagepb.FinalizeWriteStreamRequest{
		Name: w.writeStream.Name,
	}
	_, err := w.client.client.FinalizeWriteStream(context.Background(), finalizeRequest)
	if err != nil {
		return fmt.Errorf("failed to finalize write stream: %w", err)
	}

	return nil
}

func (w *BigQueryRecordWriter) WriteToBigQuery(record arrow.Record) error {
	return w.Write(record)
}

func (w *BigQueryRecordWriter) GetWriteStream() *storagepb.WriteStream {
	return w.writeStream
}
