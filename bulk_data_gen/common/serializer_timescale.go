package common

import (
	"encoding/binary"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/timescale_serializaition"
	"io"
	"log"
	"reflect"
)

type SerializerTimescaleSql struct {
}

func NewSerializerTimescaleSql() *SerializerTimescaleSql {
	return &SerializerTimescaleSql{}
}

type SerializerTimescaleBin struct {
}

func NewSerializerTimescaleBin() *SerializerTimescaleBin {
	return &SerializerTimescaleBin{}
}

// SerializePoint writes Point data to the given writer, conforming to the
// Timescale insert format.
//
// This function writes output that looks like:
// INSERT INTO <tablename> (time,<tag_name list>,<field_name list>') VALUES (<timestamp in nanoseconds>, <tag values list>, <field values>)
func (s *SerializerTimescaleSql) SerializePoint(w io.Writer, p *Point) (err error) {
	timestampNanos := p.Timestamp.UTC().UnixNano()
	buf := make([]byte, 0, 4096)
	buf = append(buf, []byte("INSERT INTO ")...)
	buf = append(buf, []byte(p.MeasurementName)...)
	buf = append(buf, []byte(" (time")...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, ","...)
		buf = append(buf, p.TagKeys[i]...)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, ","...)
		buf = append(buf, p.FieldKeys[i]...)
	}
	buf = append(buf, []byte(") VALUES (")...)
	buf = append(buf, []byte(fmt.Sprintf("%d", timestampNanos))...)

	for i := 0; i < len(p.TagValues); i++ {
		buf = append(buf, ",'"...)
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, byte('\''))
	}

	for i := 0; i < len(p.FieldValues); i++ {
		buf = append(buf, ","...)
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, true)
	}
	buf = append(buf, []byte(");\n")...)

	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerTimescaleSql) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}

// SerializeTimeScaleBin writes Point data to the given writer, conforming to the
// Binary GOP encoded format to write
//
//
func (t *SerializerTimescaleBin) SerializePoint(w io.Writer, p *Point) (err error) {

	var f timescale_serialization.FlatPoint
	f.MeasurementName = string(p.MeasurementName)
	// Write the batch.
	f.Columns = make([]string, len(p.TagKeys)+len(p.FieldKeys)+1)
	c := 0
	for i := 0; i < len(p.TagKeys); i++ {
		f.Columns[c] = string(p.TagKeys[i])
		c++
	}
	for i := 0; i < len(p.FieldKeys); i++ {
		f.Columns[c] = string(p.FieldKeys[i])
		c++
	}
	f.Columns[c] = "time"

	c = 0
	f.Values = make([]*timescale_serialization.FlatPoint_FlatPointValue, len(p.TagValues)+len(p.FieldValues)+1)
	for i := 0; i < len(p.TagValues); i++ {
		v := timescale_serialization.FlatPoint_FlatPointValue{}
		v.Type = timescale_serialization.FlatPoint_STRING
		v.StringVal = string(p.TagValues[i])
		f.Values[c] = &v
		c++
	}
	for i := 0; i < len(p.FieldValues); i++ {
		v := timescale_serialization.FlatPoint_FlatPointValue{}
		switch p.FieldValues[i].(type) {
		case int64:
			v.Type = timescale_serialization.FlatPoint_INTEGER
			v.IntVal = p.FieldValues[i].(int64)
			break
		case int:
			v.Type = timescale_serialization.FlatPoint_INTEGER
			v.IntVal = int64(p.FieldValues[i].(int))
			break
		case float64:
			v.Type = timescale_serialization.FlatPoint_FLOAT
			v.DoubleVal = p.FieldValues[i].(float64)
			break
		case string:
			v.Type = timescale_serialization.FlatPoint_STRING
			v.StringVal = p.FieldValues[i].(string)
			break
		default:
			panic(fmt.Sprintf("logic error in timescale serialization, %s", reflect.TypeOf(v)))
		}
		f.Values[c] = &v
		c++
	}
	timeVal := timescale_serialization.FlatPoint_FlatPointValue{}
	timeVal.Type = timescale_serialization.FlatPoint_INTEGER
	timeVal.IntVal = p.Timestamp.UnixNano()
	f.Values[c] = &timeVal

	out, err := f.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	s := uint64(len(out))
	binary.Write(w, binary.LittleEndian, s)
	w.Write(out)
	return nil
}

func (s *SerializerTimescaleBin) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}
