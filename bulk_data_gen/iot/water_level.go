package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	WaterLevelByteString = []byte("water_level") // heap optimization
)

var (
	// Field keys for 'air quality indoor' points.
	WaterLevelFieldKeys = [][]byte{
		[]byte("level"),
		[]byte("battery_voltage"),
	}
)

type WaterLevelMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewWaterLevelMeasurement(start time.Time, id []byte) *WaterLevelMeasurement {
	distributions := make([]Distribution, len(WaterLevelFieldKeys))
	//level
	distributions[0] = MUDWD(ND(0, 1), 0.0, 8000, 5000)
	//battery_voltage
	distributions[1] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &WaterLevelMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *WaterLevelMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *WaterLevelMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(WaterLevelByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(WaterLevelFieldKeys[i], m.distributions[i].Get())
	}
	return true
}
