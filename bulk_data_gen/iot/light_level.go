package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	LightLevelRoomByteString = []byte("light_level_room") // heap optimization
)

var (
	// Field keys for 'air quality indoor' points.
	LightLevelRoomFieldKeys = [][]byte{
		[]byte("level"),
		[]byte("battery_voltage"),
	}
)

type LightLevelRoomMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewLightLevelRoomMeasurement(start time.Time, id []byte) *LightLevelRoomMeasurement {
	distributions := make([]Distribution, len(LightLevelRoomFieldKeys))
	//level
	distributions[0] = MUDWD(ND(0, 1), 0.00001, 1e5, 10000)
	//battery_voltage
	distributions[1] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &LightLevelRoomMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *LightLevelRoomMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *LightLevelRoomMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(LightLevelRoomByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(LightLevelRoomFieldKeys[i], m.distributions[i].Get())
	}
	return true
}
