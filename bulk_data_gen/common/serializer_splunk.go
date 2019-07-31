package common

import (
	"bytes"
	"fmt"
	"io"
)

type SerializerSplunkJson struct {
	buf []byte
}

func NewSerializerSplunkJson() *SerializerSplunkJson {
	return &SerializerSplunkJson{
		buf:make([]byte, 0, 4096),
	}
}

// SerializePoint writes Point data to the given writer, conforming to the
// Splunk JSON format.
//
// This function writes output that looks like:
// ...
func (s *SerializerSplunkJson) SerializePoint(w io.Writer, p *Point) (err error) {
	timestamp := p.Timestamp.UTC().Unix()
	buf := s.buf[:0]
	var host []byte
	for i := 0; i < len(p.TagKeys); i++ {
		if bytes.Compare(p.TagKeys[i], []byte("hostname")) == 0 {
			host = p.TagValues[i]
			break
		}
	}
	timestampPart := fmt.Sprintf("\"time\":%d,", timestamp)
	sourcePart := fmt.Sprintf("\"source\":\"%s\",", p.MeasurementName)
	hostPart := fmt.Sprintf("\"host\":\"%s\",", string(host))
	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, "{"...)
		buf = append(buf, []byte(timestampPart)...)
		buf = append(buf, []byte("\"event\":\"metric\",")...)
		buf = append(buf, []byte(sourcePart)...)
		buf = append(buf, []byte(hostPart)...)
		buf = append(buf, []byte("\"fields\":{",)...)
		for i := 0; i < len(p.TagKeys); i++ {
			buf = append(buf, "\""...)
			buf = append(buf, p.TagKeys[i]...)
			buf = append(buf, "\":\""...)
			buf = append(buf, p.TagValues[i]...)
			buf = append(buf, "\","...)
		}
		buf = append(buf, "\"_value\":"...)
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, false)
		buf = append(buf, ",\"metric_name\":\""...)
		buf = append(buf, p.MeasurementName...)
		buf = append(buf, "."...)
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, "\""...)
		buf = append(buf, "}}\n"...)
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerSplunkJson) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}
