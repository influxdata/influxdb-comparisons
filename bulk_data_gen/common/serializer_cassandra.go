package common

import (
	"fmt"
	"io"
)

type SerializerCassandra struct {
}

func NewSerializerCassandra() *SerializerCassandra {
	return &SerializerCassandra{}
}

// SerializeCassandra writes Point data to the given writer, conforming to the
// Cassandra query format.
//
// This function writes output that looks like:
// INSERT INTO <tablename> (series_id, ts_ns, value) VALUES (<series_id>, <timestamp_nanoseconds>, <field value>)
// where series_id looks like: <measurement>,<tagset>#<field name>#<time shard>
//
// For example:
// INSERT INTO all_series (series_id, timestamp_ns, value) VALUES ('cpu,hostname=host_01#user#2016-01-01', 12345, 42.1)\n
func (m *SerializerCassandra) SerializePoint(w io.Writer, p *Point) (err error) {
	timestampNanos := p.Timestamp.UTC().UnixNano()
	buf := make([]byte, 0, 4096)
	buf = append(buf, []byte("INSERT INTO measurements.")...)
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
		buf = fastFormatAppendCassandra(v, buf, true)
	}
	buf = append(buf, []byte(");\n")...)

	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func fastFormatAppendCassandra(v interface{}, buf []byte, singleQuotesForString bool) []byte {
	switch v.(type) {
	case []byte, string:
		buf = append(buf, []byte("textasblob(")...)
		buf = fastFormatAppend(v, buf, singleQuotesForString)
		buf = append(buf, []byte(")")...)
		return buf
	default:
		return fastFormatAppend(v, buf, singleQuotesForString)
	}
}

func typeNameForCassandra(v interface{}) string {
	switch v.(type) {
	case int, int64:
		return "bigint"
	case float64:
		return "double"
	case float32:
		return "float"
	case bool:
		return "boolean"
	case []byte, string:
		return "blob"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

func (s *SerializerCassandra) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}
