package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	AirQualityRoomByteString = []byte("air_quality_room") // heap optimization
)

var (
	// Field keys for 'air quality indoor' points.
	AirQualityRoomFieldKeys = [][]byte{
		[]byte("co2_level"),
		[]byte("co_level"),
		[]byte("battery_voltage"),
	}
)

type AirQualityRoomMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewAirQualityRoomMeasurement(start time.Time, id []byte) *AirQualityRoomMeasurement {
	distributions := make([]Distribution, len(AirQualityRoomFieldKeys))
	//co2_level
	distributions[0] = MUDWD(ND(0, 1), 200, 3000, 300)
	//co_level
	distributions[1] = MUDWD(ND(0.001, 0.0001), 0, 10, 0)
	//battery_voltage
	distributions[2] = MUDWD(ND(1, 0.5), 1, 3.2, 3.2)

	return &AirQualityRoomMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *AirQualityRoomMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *AirQualityRoomMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(AirQualityRoomByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(AirQualityRoomFieldKeys[i], m.distributions[i].Get())
	}
}
