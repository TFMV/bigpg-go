package arrowpb

import (
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	arrowproto "github.com/TFMV/bigpg-go/proto"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
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

		// Extract values based on actual schema
		for j := 0; j < int(record.NumCols()); j++ {
			col := record.Column(j)
			if col.IsNull(i) {
				continue
			}

			switch record.ColumnName(j) {
			case "dep_delay":
				row.DepDelay = col.(*array.Int64).Value(i)
			case "arr_delay":
				row.ArrDelay = col.(*array.Int64).Value(i)
			case "air_time":
				row.AirTime = col.(*array.Int64).Value(i)
			case "distance":
				row.Distance = col.(*array.Int64).Value(i)
			case "dep_time":
				row.DepTime = col.(*array.Float64).Value(i)
			case "arr_time":
				row.ArrTime = col.(*array.Float64).Value(i)
			}
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
			Name:   &field.Name,
			Type:   protoType.Enum(),
			Number: proto.Int32(int32(i + 1)), // Add field numbers starting from 1
		}
	}

	name := string(fileDescriptor.Name())
	return &storagepb.ProtoSchema{
		ProtoDescriptor: &descriptorpb.DescriptorProto{
			Name:    &name,
			Field:   fields,
			Options: &descriptorpb.MessageOptions{},
		},
	}
}
