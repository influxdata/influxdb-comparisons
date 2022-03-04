package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/google/flatbuffers/go"
	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
)

type SerializerMongo struct {
}

func NewSerializerMongo() *SerializerMongo {
	return &SerializerMongo{}
}

// SerializeMongo writes Point data to the given writer, conforming to the
// mongo_serialization FlatBuffers format.
func (s *SerializerMongo) SerializePoint(w io.Writer, p *Point) (err error) {
	// Prepare the series id prefix, which is the set of tags associated
	// with this point. The series id prefix is the base of each value's
	// particular collection name:
	lenBuf := bufPool8.Get().([]byte)

	// Prepare the timestamp, which is the same for each value in this
	// Point:
	timestampNanos := p.Timestamp.UTC().UnixNano()

	// Fetch a flatbuffers builder from a pool:
	builder := fbBuilderPool.Get().(*flatbuffers.Builder)

	// For each field in this Point, serialize its:
	// collection name (series id prefix + the name of the value)
	// timestamp in nanos (int64)
	// numeric value (int, int64, or float64 -- determined by reflection)
	tagOffsets := make([]flatbuffers.UOffsetT, 0, len(p.TagKeys))
	fieldOffsets := make([]flatbuffers.UOffsetT, 0, len(p.FieldKeys))

	// write the tag data, which must be separate:
	for i := 0; i < len(p.TagKeys); i++ {
		keyData := builder.CreateByteVector(p.TagKeys[i])
		valData := builder.CreateByteVector(p.TagValues[i])
		mongo_serialization.TagStart(builder)
		mongo_serialization.TagAddKey(builder, keyData)
		mongo_serialization.TagAddVal(builder, valData)
		tagOffset := mongo_serialization.TagEnd(builder)
		tagOffsets = append(tagOffsets, tagOffset)
	}
	mongo_serialization.ItemStartTagsVector(builder, len(tagOffsets))
	for _, tagOffset := range tagOffsets {
		builder.PrependUOffsetT(tagOffset)
	}
	tagsVecOffset := builder.EndVector(len(tagOffsets))

	// write the field data, which must be separate:
	for i := 0; i < len(p.FieldKeys); i++ {
		keyData := builder.CreateByteVector(p.FieldKeys[i])
		mongo_serialization.FieldStart(builder)
		mongo_serialization.FieldAddKey(builder, keyData)
		genericValue := p.FieldValues[i]
		switch v := genericValue.(type) {
		// (We can't switch on sets of types (e.g. int, int64) because that does not make v concrete.)
		case int:
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeInt)
			mongo_serialization.FieldAddIntValue(builder, int32(v))
		case int32:
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeInt)
			mongo_serialization.FieldAddIntValue(builder, v)
		case int64:
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeLong)
			mongo_serialization.FieldAddLongValue(builder, v)
		case float32:
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeFloat)
			mongo_serialization.FieldAddFloatValue(builder, v)
		case float64:
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeDouble)
			mongo_serialization.FieldAddDoubleValue(builder, v)
		case string, []byte:
			var stringOffset flatbuffers.UOffsetT
			switch v2 := v.(type) {
			case string:
				stringOffset = builder.CreateString(v2)
			case []byte:
				stringOffset = builder.CreateByteVector(v2)
			}
			mongo_serialization.FieldAddValueType(builder, mongo_serialization.ValueTypeString)
			mongo_serialization.FieldAddStringValue(builder, stringOffset)
		default:
			panic(fmt.Sprintf("logic error in mongo serialization, %s", reflect.TypeOf(v)))
		}
		fieldOffset := mongo_serialization.FieldEnd(builder)
		fieldOffsets = append(fieldOffsets, fieldOffset)
	}
	mongo_serialization.ItemStartFieldsVector(builder, len(fieldOffsets))
	for _, fieldOffset := range fieldOffsets {
		builder.PrependUOffsetT(fieldOffset)
	}
	fieldsVecOffset := builder.EndVector(len(fieldOffsets))

	// build the flatbuffer representing this point:
	measurementNameOffset := builder.CreateByteVector(p.MeasurementName)

	mongo_serialization.ItemStart(builder)
	mongo_serialization.ItemAddTimestampNanos(builder, timestampNanos)
	mongo_serialization.ItemAddMeasurementName(builder, measurementNameOffset)
	mongo_serialization.ItemAddTags(builder, tagsVecOffset)
	mongo_serialization.ItemAddFields(builder, fieldsVecOffset)

	rootTable := mongo_serialization.ItemEnd(builder)
	builder.Finish(rootTable)

	// Access the finished byte slice representing this flatbuffer:
	buf := builder.FinishedBytes()

	// Write the metadata for the flatbuffer object:
	binary.LittleEndian.PutUint64(lenBuf, uint64(len(buf)))
	_, err = w.Write(lenBuf)
	if err != nil {
		return err
	}

	// Write the flatbuffer object:
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	// Give the flatbuffers builder back to a pool:
	builder.Reset()
	fbBuilderPool.Put(builder)

	// Give the 8-byte buf back to a pool:
	bufPool8.Put(lenBuf)

	return nil
}

func (s *SerializerMongo) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}
