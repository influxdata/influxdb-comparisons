package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	AirConditionOutdoorByteString = []byte("air_condition_outdoor") // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	AirConditionOutdoorFieldKeys = [][]byte{
		[]byte("temperature"),
		[]byte("humidity"),
		[]byte("battery_voltage"),
	}
)

type AirConditionOutdoorMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewAirConditionOutdoorMeasurement(start time.Time, id []byte) *AirConditionOutdoorMeasurement {
	distributions := make([]Distribution, len(AirConditionOutdoorFieldKeys))
	//temperature
	distributions[0] = MUDWD(ND(0, 1), -20, 28, 0)
	//humidity
	distributions[1] = MUDWD(ND(0, 1), 5, 95, 80)
	//battery_voltage
	distributions[2] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &AirConditionOutdoorMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *AirConditionOutdoorMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *AirConditionOutdoorMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(AirConditionOutdoorByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(AirConditionOutdoorFieldKeys[i], m.distributions[i].Get())
	}
}
