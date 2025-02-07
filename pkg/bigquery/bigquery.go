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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/api/option"
)

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

	return &BigQueryWriteClient{
		client: client,
		schema: schema,
	}, nil
}

type BigQueryRecordWriter struct {
	client        *BigQueryWriteClient
	appendClient  storagepb.BigQueryWrite_AppendRowsClient
	writeStream   *storagepb.WriteStream
	protoSchema   *storagepb.ProtoSchema
	buffer        *bytes.Buffer
	ipcWriter     *ipc.Writer
	writeDone     sync.WaitGroup
	writerOptions *BigQueryWriteOptions
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

	buffer := &bytes.Buffer{}
	ipcWriter := ipc.NewWriter(buffer, ipc.WithSchema(client.schema), ipc.WithAllocator(opts.Allocator))
	protoSchema := arrowpb.ArrowSchemaToProto(client.schema)

	return &BigQueryRecordWriter{
		client:        client,
		appendClient:  appendClient,
		writeStream:   writeStream,
		protoSchema:   protoSchema,
		buffer:        buffer,
		ipcWriter:     ipcWriter,
		writerOptions: opts,
	}, nil
}

func (w *BigQueryRecordWriter) Write(record arrow.Record) error {
	if !w.client.schema.Equal(record.Schema()) {
		return fmt.Errorf("schema mismatch: expected %v but got %v", w.client.schema, record.Schema())
	}

	w.buffer.Reset()

	if err := w.ipcWriter.Write(record); err != nil {
		return fmt.Errorf("error writing record to buffer: %w", err)
	}

	serializedData := w.buffer.Bytes()
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
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := w.appendClient.Send(appendReq)
		if err == nil {
			fmt.Printf("AppendRowsRequest sent successfully on attempt %d\n", attempt+1)
			return nil // Success, exit the function
		}

		lastErr = err
		fmt.Printf("Error sending AppendRowsRequest (attempt %d of %d): %v\n", attempt+1, maxRetries, err)

		if err == io.EOF {
			if err := w.recreateAppendClient(); err != nil {
				return fmt.Errorf("failed to recreate append client: %w", err)
			}
			continue
		}

		// Add a small delay before retrying
		time.Sleep(time.Second * time.Duration(attempt+1))
	}

	return fmt.Errorf("failed to send AppendRowsRequest after %d attempts: %w", maxRetries, lastErr)
}

func (w *BigQueryRecordWriter) recreateAppendClient() error {
	var err error
	ctx := context.Background()
	w.appendClient, err = w.client.client.AppendRows(ctx)
	return err
}

func (w *BigQueryRecordWriter) Close() error {
	w.writeDone.Wait()

	if err := w.ipcWriter.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}

	// Finalize the write stream
	finalizeRequest := &storagepb.FinalizeWriteStreamRequest{
		Name: w.writeStream.Name,
	}
	_, err := w.client.client.FinalizeWriteStream(context.Background(), finalizeRequest)
	if err != nil {
		return fmt.Errorf("failed to finalize write stream: %w", err)
	}

	defer memoryPool.PutAllocator(w.writerOptions.Allocator)

	return nil
}
