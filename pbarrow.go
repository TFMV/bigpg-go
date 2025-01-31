package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TypeMappings maps Apache Arrow types to Protocol Buffers field types.
var TypeMappings = map[arrow.DataType]descriptorpb.FieldDescriptorProto_Type{
	arrow.BinaryTypes.Binary:          descriptorpb.FieldDescriptorProto_TYPE_BYTES,
	arrow.FixedWidthTypes.Boolean:     descriptorpb.FieldDescriptorProto_TYPE_BOOL,
	arrow.PrimitiveTypes.Float64:      descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
	arrow.PrimitiveTypes.Int64:        descriptorpb.FieldDescriptorProto_TYPE_INT64,
	arrow.BinaryTypes.String:          descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.FixedWidthTypes.Date32:      descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.FixedWidthTypes.Timestamp_s: descriptorpb.FieldDescriptorProto_TYPE_STRING, // Fix
}

// generateUniqueName creates a unique name for the ProtoBuf message.
func generateUniqueName(prefix string) string {
	timestamp := time.Now().Unix()
	randomSuffix := rand.Intn(9999)
	return fmt.Sprintf("%s_%d_%04d", prefix, timestamp, randomSuffix)
}

// createNestedMessage constructs a nested ProtoBuf message descriptor.
func createNestedMessage(fieldType *arrow.StructType, messageName string) *descriptorpb.DescriptorProto {
	nestedDescriptor := &descriptorpb.DescriptorProto{Name: &messageName}

	for i, field := range fieldType.Fields() {
		fieldProto := &descriptorpb.FieldDescriptorProto{
			Name:   &field.Name,
			Number: proto.Int32(int32(i + 1)),
		}

		if protoType, exists := TypeMappings[field.Type]; exists {
			fieldProto.Type = &protoType
		} else {
			log.Fatalf("Unsupported nested type: %v", field.Type)
		}

		nestedDescriptor.Field = append(nestedDescriptor.Field, fieldProto)
	}
	return nestedDescriptor
}

// arrowSchemaToProto converts an Arrow schema into a Protocol Buffers descriptor.
func arrowSchemaToProto(schema *arrow.Schema) *descriptorpb.DescriptorProto {
	messageName := generateUniqueName("ArrowMessage")

	descriptorProto := &descriptorpb.DescriptorProto{Name: &messageName}

	// Process nested struct fields first
	nestedTypes := make(map[string]*descriptorpb.DescriptorProto)
	for _, field := range schema.Fields() {
		if structType, ok := field.Type.(*arrow.StructType); ok {
			nestedName := field.Name + "Type"
			nestedMessage := createNestedMessage(structType, nestedName)
			nestedTypes[field.Name] = nestedMessage
			descriptorProto.NestedType = append(descriptorProto.NestedType, nestedMessage)
		}
	}

	// Process primary fields
	for i, field := range schema.Fields() {
		fieldProto := &descriptorpb.FieldDescriptorProto{
			Name:   &field.Name,
			Number: proto.Int32(int32(i + 1)),
		}

		if nested, exists := nestedTypes[field.Name]; exists {
			messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
			fieldProto.Type = &messageType
			fieldProto.TypeName = nested.Name
		} else if protoType, exists := TypeMappings[field.Type]; exists {
			fieldProto.Type = &protoType
		} else {
			log.Fatalf("Unsupported Arrow type: %v", field.Type)
		}

		descriptorProto.Field = append(descriptorProto.Field, fieldProto)
	}

	return descriptorProto
}

// extractValue retrieves a value from an Arrow column at a specific row index.
func extractValue(col arrow.Array, rowIndex int) interface{} {
	switch col := col.(type) {
	case *array.Int64:
		return col.Value(rowIndex)
	case *array.Float64:
		return col.Value(rowIndex)
	case *array.String:
		return col.Value(rowIndex)
	case *array.Boolean:
		return col.Value(rowIndex)
	default:
		return nil
	}
}

// createArrowRecord creates a sample Arrow RecordBatch for testing.
func createArrowRecord() (array.RecordReader, error) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Append sample data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2, 76.8}, nil)

	record := builder.NewRecord()
	return array.NewRecordReader(schema, []arrow.Record{record})
}

// FormatArrowJSON formats Arrow records as pretty-printed JSON
func FormatArrowJSON(reader array.RecordReader, output io.Writer) error {
	defer reader.Release()

	// Start JSON array
	if _, err := output.Write([]byte("[\n")); err != nil {
		return fmt.Errorf("failed to write JSON start: %w", err)
	}

	first := true
	for reader.Next() {
		if !first {
			if _, err := output.Write([]byte(",\n")); err != nil {
				return fmt.Errorf("failed to write separator: %w", err)
			}
		}
		first = false

		record := reader.Record()
		if err := formatRecord(record, output); err != nil {
			return err
		}
	}

	// Check for errors during iteration
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading records: %w", err)
	}

	// Close JSON array
	if _, err := output.Write([]byte("\n]\n")); err != nil {
		return fmt.Errorf("failed to write JSON end: %w", err)
	}

	return nil
}

func formatRecord(record arrow.Record, output io.Writer) error {
	// Convert record to JSON
	buf := new(bytes.Buffer)
	if err := array.RecordToJSON(record, buf); err != nil {
		return fmt.Errorf("failed to convert record to JSON: %w", err)
	}

	// Format with indentation
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, buf.Bytes(), "  ", "  "); err != nil {
		return fmt.Errorf("failed to format JSON: %w", err)
	}

	// Write formatted JSON
	if _, err := output.Write(prettyJSON.Bytes()); err != nil {
		return fmt.Errorf("failed to write JSON: %w", err)
	}

	return nil
}

func main() {
	log.Println("Converting Arrow schema to ProtoBuf descriptor...")

	// Generate sample Arrow RecordBatch
	record, err := createArrowRecord()
	if err != nil {
		log.Fatalf("Failed to create Arrow RecordBatch: %v", err)
	}
	defer record.Release()

	// Convert Arrow Schema to ProtoBuf
	protoDescriptor := arrowSchemaToProto(record.Schema())
	fmt.Printf("Proto Descriptor: %v\n", protoDescriptor)

	log.Println("Converted Arrow schema to ProtoBuf descriptor successfully!")
}
