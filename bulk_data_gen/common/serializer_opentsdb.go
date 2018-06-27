package common

import (
	"encoding/json"
	"fmt"
	"io"
)

type SerializerOpenTSDB struct {
}

func NewSerializerOpenTSDB() *SerializerOpenTSDB {
	return &SerializerOpenTSDB{}
}

// SerializeOpenTSDBBulk writes Point data to the given writer, conforming to
// the OpenTSDB bulk load protocol (the /api/put endpoint). Note that no line
// has a trailing comma. Downstream programs are responsible for creating
// batches for POSTing using a JSON array, and for adding any trailing commas
// (to conform to JSON). We use only millisecond-precision timestamps.
//
// N.B. OpenTSDB only supports millisecond or second resolution timestamps.
// N.B. OpenTSDB millisecond timestamps must be 13 digits long.
// N.B. OpenTSDB only supports floating-point field values.
//
// This function writes JSON lines that looks like:
// { <metric>, <timestamp>, <value>, <tags> }
//
// For example:
// { "metric": "cpu.usage_user", "timestamp": 14516064000000, "value": 99.5170917755353770, "tags": { "hostname": "host_01", "region": "ap-southeast-2", "datacenter": "ap-southeast-2a" } }
func (m *SerializerOpenTSDB) SerializePoint(w io.Writer, p *Point) (err error) {
	type wirePoint struct {
		Metric    string            `json:"metric"`
		Timestamp int64             `json:"timestamp"`
		Tags      map[string]string `json:"tags"`
		Value     float64           `json:"value"`
	}

	metricBase := string(p.MeasurementName) // will be re-used
	encoder := json.NewEncoder(w)

	wp := wirePoint{}
	// Timestamps in OpenTSDB must be millisecond precision:
	wp.Timestamp = p.Timestamp.UTC().UnixNano() / 1e6
	// sanity check
	{
		x := fmt.Sprintf("%d", wp.Timestamp)
		if len(x) != 13 {
			panic("serialized timestamp was not 13 digits")
		}
	}
	wp.Tags = make(map[string]string, len(p.TagKeys))
	for i := 0; i < len(p.TagKeys); i++ {
		// so many allocs..
		key := string(p.TagKeys[i])
		val := string(p.TagValues[i])
		wp.Tags[key] = val
	}

	// for each Value, generate a new line in the output:
	for i := 0; i < len(p.FieldKeys); i++ {
		wp.Metric = metricBase + "." + string(p.FieldKeys[i])
		switch x := p.FieldValues[i].(type) {
		case int:
			wp.Value = float64(x)
		case int64:
			wp.Value = float64(x)
		case float32:
			wp.Value = float64(x)
		case float64:
			wp.Value = float64(x)
		default:
			panic("bad numeric value for OpenTSDB serialization")
		}

		err := encoder.Encode(wp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SerializerOpenTSDB) SerializeSize(w io.Writer, points int64, values int64) error {
	//return serializeSizeInText(w, points, values)
	return nil
}
