package common

import (
	"fmt"
	"io"
)

type SerializerGraphiteLine struct {
	buf []byte
}

func NewSerializerGraphiteLine() *SerializerGraphiteLine {
	return &SerializerGraphiteLine{
		buf:make([]byte, 0, 4096),
	}
}

// SerializePoint writes Point data to the given writer, conforming to the
// Graphite plain text line protocol.
func (s *SerializerGraphiteLine) SerializePoint(w io.Writer, p *Point) (err error) {
	timestamp := p.Timestamp.UTC().Unix()
	buf := s.buf[:0]
	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, []byte(p.MeasurementName)...)
		buf = append(buf, "."...)
		buf = append(buf, p.FieldKeys[i]...)
		for i := 0; i < len(p.TagKeys); i++ {
			buf = append(buf, ";"...)
			buf = append(buf, p.TagKeys[i]...)
			buf = append(buf, "="...)
			buf = append(buf, p.TagValues[i]...)
		}
		buf = append(buf, " "...)
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf, true)
		buf = append(buf, " "...)
		buf = append(buf, []byte(fmt.Sprintf("%d", timestamp))...)
		buf = append(buf, "\n"...)
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func (s *SerializerGraphiteLine) SerializeSize(w io.Writer, points int64, values int64) error {
	return serializeSizeInText(w, points, values)
}
