// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package utils

import (
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func ConvertSchemaSPB(schema *arrow.Schema) *storagepb.ProtoSchema {
	if schema == nil {
		return nil
	}

	fields := make([]*descriptorpb.FieldDescriptorProto, len(schema.Fields()))

	for i, field := range schema.Fields() {
		fields[i] = encodeField(field, int32(i+1))
	}

	return &storagepb.ProtoSchema{
		ProtoDescriptor: &descriptorpb.DescriptorProto{
			Name:  proto.String("ArrowSchema"),
			Field: fields,
		},
	}
}

// encodeField converts an Arrow field to a descriptorpb.FieldDescriptorProto.
func encodeField(field arrow.Field, fieldNumber int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(field.Name),
		Number: proto.Int32(fieldNumber),
		Type:   MapArrowTypeToProtoType(field.Type),
	}
}

// DecodeToMap decodes a protobuf Struct to a Go map.
func DecodeToMap(s *pb.Struct) map[string]interface{} {
	if s == nil {
		return nil
	}
	m := map[string]interface{}{}
	for k, v := range s.Fields {
		m[k] = decodeValue(v)
	}
	return m
}

// decodeValue decodes a protobuf Value to an appropriate Go type.
func decodeValue(v *pb.Value) interface{} {
	switch k := v.Kind.(type) {
	case *pb.Value_NullValue:
		return nil
	case *pb.Value_NumberValue:
		return k.NumberValue
	case *pb.Value_StringValue:
		return k.StringValue
	case *pb.Value_BoolValue:
		return k.BoolValue
	case *pb.Value_StructValue:
		return DecodeToMap(k.StructValue)
	case *pb.Value_ListValue:
		s := make([]interface{}, len(k.ListValue.Values))
		for i, e := range k.ListValue.Values {
			s[i] = decodeValue(e)
		}
		return s
	default:
		panic("protostruct: unknown kind")
	}
}

// MapArrowTypeToProtoType maps an Arrow data type to a protobuf field type.
func MapArrowTypeToProtoType(dataType arrow.DataType) *descriptorpb.FieldDescriptorProto_Type {
	switch dataType.(type) {
	case *arrow.Int32Type:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	case *arrow.Int64Type:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	case *arrow.Float32Type:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum()
	case *arrow.Float64Type:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	case *arrow.StringType:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	case *arrow.BooleanType:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	case *arrow.BinaryType:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	case *arrow.Date32Type:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	case *arrow.Date64Type:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	case *arrow.TimestampType:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	case *arrow.Time32Type:
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED32.Enum()
	case *arrow.Time64Type:
		return descriptorpb.FieldDescriptorProto_TYPE_FIXED64.Enum()
	case *arrow.DurationType:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	// Add more cases as needed for other Arrow types
	default:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum() // Default to string if the type is not matched
	}
}

func IsCompatibleProtoType(protoType descriptorpb.FieldDescriptorProto_Type, value interface{}) bool {
	switch protoType {
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		_, ok := value.(bool)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_INT64:
		_, ok := value.(int64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		_, ok := value.(string)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
		_, ok := value.(float64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
		_, ok := value.(float64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
		_, ok := value.([]byte)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
		_, ok := value.(uint32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32:
		_, ok := value.(uint32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64:
		_, ok := value.(uint64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_INT32:
		_, ok := value.(int32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
		_, ok := value.(uint64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32:
		_, ok := value.(int32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64:
		_, ok := value.(int64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32:
		_, ok := value.(int32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64:
		_, ok := value.(int64)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		_, ok := value.(int32)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		_, ok := value.(*pb.Struct)
		return ok
	case descriptorpb.FieldDescriptorProto_TYPE_GROUP:
		_, ok := value.(*pb.Struct)
		return ok
	default:
		return false
	}
}
