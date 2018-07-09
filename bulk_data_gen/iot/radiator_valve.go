package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	RadiatorValveRoomByteString = []byte("radiator_valve_room") // heap optimization
	RadiatorTagKey              = []byte("radiator")
)

var (
	// Field keys for 'air quality indoor' points.
	RadiatorValveRoomFieldKeys = [][]byte{
		[]byte("opening_level"),
		[]byte("battery_voltage"),
	}
)

type RadiatorValveRoomMeasurement struct {
	sensorId      []byte
	randiatorId   []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewRadiatorValveRoomMeasurement(start time.Time, randiatorId []byte, sensorId []byte) *RadiatorValveRoomMeasurement {
	distributions := make([]Distribution, len(RadiatorValveRoomFieldKeys))
	//opening_level
	distributions[0] = CWD(ND(0, 1), 0.0, 100, 0)
	//battery_voltage
	distributions[1] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &RadiatorValveRoomMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      sensorId,
		randiatorId:   randiatorId,
	}
}

func (m *RadiatorValveRoomMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *RadiatorValveRoomMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(RadiatorValveRoomByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(RadiatorTagKey, m.randiatorId)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(RadiatorValveRoomFieldKeys[i], m.distributions[i].Get())
	}
	return true
}
