package arrowpb

import (
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	arrowproto "github.com/TFMV/bigpg-go/proto"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Map Arrow types to Protobuf types
var typeMapping = map[arrow.DataType]descriptorpb.FieldDescriptorProto_Type{
	arrow.PrimitiveTypes.Int64:   descriptorpb.FieldDescriptorProto_TYPE_INT64,
	arrow.BinaryTypes.String:     descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.PrimitiveTypes.Float64: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
}

// ConvertArrowRecord converts an Arrow RecordBatch into BigQuery Protobuf rows
func ConvertArrowRecord(record arrow.Record) ([]*arrowproto.Row, error) {
	rows := make([]*arrowproto.Row, record.NumRows())

	for i := 0; i < int(record.NumRows()); i++ {
		row := &arrowproto.Row{}

		// Extract ID (Int64)
		idCol := record.Column(0).(*array.Int64)
		if idCol.IsValid(i) {
			row.Id = idCol.Value(i)
		}

		// Extract Name (String)
		nameCol := record.Column(1).(*array.String)
		if nameCol.IsValid(i) {
			row.Name = nameCol.Value(i)
		}

		// Extract Score (Float64)
		scoreCol := record.Column(2).(*array.Float64)
		if scoreCol.IsValid(i) {
			row.Score = scoreCol.Value(i)
		}

		// Extract CreatedAt (Timestamp)
		tsCol := record.Column(3).(*array.Timestamp)
		if tsCol.IsValid(i) {
			row.CreatedAt = timestamppb.New(tsCol.Value(i).ToTime(arrow.Nanosecond))
		}

		rows[i] = row
	}

	return rows, nil
}

// SerializeRows serializes BigQuery Rows into Protobuf bytes
func SerializeRows(rows []*arrowproto.Row) ([][]byte, error) {
	out := make([][]byte, len(rows))
	for i, row := range rows {
		data, err := proto.Marshal(row)
		if err != nil {
			return nil, err
		}
		out[i] = data
	}
	return out, nil
}

// ArrowSchemaToProto converts an Arrow schema to a BigQuery ProtoSchema
func ArrowSchemaToProto(schema *arrow.Schema) *storagepb.ProtoSchema {
	// Get the Protobuf descriptor for the Row message
	msgDescriptor := (&arrowproto.Row{}).ProtoReflect().Descriptor()
	fileDescriptor := msgDescriptor.ParentFile()

	// Convert to FileDescriptorProto
	fields := make([]*descriptorpb.FieldDescriptorProto, len(schema.Fields()))

	for i, field := range schema.Fields() {
		protoType := typeMapping[field.Type]
		fields[i] = &descriptorpb.FieldDescriptorProto{
			Name: &field.Name,
			Type: protoType.Enum(),
		}
	}

	options := &descriptorpb.MessageOptions{}

	name := string(fileDescriptor.Name())
	return &storagepb.ProtoSchema{
		ProtoDescriptor: &descriptorpb.DescriptorProto{
			Name:    &name,
			Field:   fields,
			Options: options,
		},
	}
}
