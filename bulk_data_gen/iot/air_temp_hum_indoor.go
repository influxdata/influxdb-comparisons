package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	AirConditionRoomByteString = []byte("air_condition_room") // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	AirConditionRoomFieldKeys = [][]byte{
		[]byte("temperature"),
		[]byte("humidity"),
		[]byte("battery_voltage"),
	}
)

type AirConditionRoomMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewAirConditionRoomMeasurement(start time.Time, id []byte) *AirConditionRoomMeasurement {
	distributions := make([]Distribution, len(AirConditionRoomFieldKeys))
	//temperature
	distributions[0] = MUDWD(ND(0, 1), 15, 28, 15)
	//humidity
	distributions[1] = MUDWD(ND(0, 1), 25, 60, 40)
	//battery_voltage
	distributions[2] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &AirConditionRoomMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *AirConditionRoomMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *AirConditionRoomMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(AirConditionRoomByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(AirConditionRoomFieldKeys[i], m.distributions[i].Get())
	}
	return true
}
