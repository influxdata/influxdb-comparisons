package common

import (
	"io"
	"strings"
)

var typeName5x = []byte("point")
var typeName6x = []byte("_doc")

type SerializerElastic struct {
	typeName []byte
}

func NewSerializerElastic(version string) *SerializerElastic {
	typeName := typeName5x
	if strings.HasPrefix(version, "6") {
		typeName = typeName6x
	}
	return &SerializerElastic{typeName: typeName}
}

// SerializeESBulk writes Point data to the given writer, conforming to the
// ElasticSearch bulk load protocol.
//
// This function writes output that looks like:
// <action line>
// <tags, fields, and timestamp>
//
// For example:
// { "index" : { "_index" : "measurement_otqio", "_type" : "point" } }\n
// { "tag_launx": "btkuw", "tag_gaijk": "jiypr", "field_wokxf": 0.08463898963964356, "field_zqstf": -0.043641533500086316, "timestamp": 171300 }\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.

func (s *SerializerElastic) SerializePoint(w io.Writer, p *Point) error {
	buf := scratchBufPool.Get().([]byte)

	buf = append(buf, "{ \"index\" : { \"_index\" : \""...)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, "\", \"_type\" : \""...)
	buf = append(buf, s.typeName...)
	buf = append(buf, "\" } }\n"...)

	buf = append(buf, '{')

	for i := 0; i < len(p.TagKeys); i++ {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, "\""...)
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, "\": \""...)
		buf = append(buf, p.TagValues[i]...)
		buf = append(buf, "\""...)
	}

	if len(p.TagKeys) > 0 && len(p.FieldKeys) > 0 {
		buf = append(buf, ',')
		buf = append(buf, ' ')
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = append(buf, '"')
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, "\": "...)

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)
	}

	if len(p.TagKeys) > 0 || len(p.FieldKeys) > 0 {
		buf = append(buf, ", "...)
	}
	// Timestamps in ES must be millisecond precision:
	buf = append(buf, "\"timestamp\": "...)
	buf = fastFormatAppend(p.Timestamp.UTC().UnixNano()/1e6, buf, true)
	buf = append(buf, " }\n"...)

	_, err := w.Write(buf)

	buf = buf[:0]
	scratchBufPool.Put(buf)

	return err
}

func (s *SerializerElastic) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}
