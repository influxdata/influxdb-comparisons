package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var HomeStates = [][]byte{
	[]byte("Empty"),
	[]byte("Half"),
	[]byte("Full"),
}

var (
	HomeStateByteString = []byte("home_state") // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	HomeStateFieldKeys = [][]byte{
		[]byte("state"),
		[]byte("state_string"),
	}
)

type HomeStateMeasurement struct {
	sensorId  []byte
	timestamp time.Time
	state     int64
}

func NewHomeStateMeasurement(start time.Time, id []byte) *HomeStateMeasurement {

	return &HomeStateMeasurement{
		timestamp: start,
		sensorId:  id,
	}
}

func (m *HomeStateMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	m.state = rand.Int63n(int64(len(HomeStates)))
}

func (m *HomeStateMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(HomeStateByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	p.AppendField(HomeStateFieldKeys[0], m.state)
	p.AppendField(HomeStateFieldKeys[1], HomeStates[m.state])
	return true
}
