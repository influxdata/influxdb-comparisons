package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	WindowByteString = []byte("window_state_room") // heap optimization
	WindowTagKey     = []byte("window")
)

var (
	// Field keys for 'air condition indoor' points.
	WindowFieldKeys = [][]byte{
		[]byte("state"),
		[]byte("battery_voltage"),
	}
)

type WindowMeasurement struct {
	sensorId      []byte
	windowId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewWindowMeasurement(start time.Time, windowId []byte, sensorId []byte) *WindowMeasurement {
	distributions := make([]Distribution, len(WindowFieldKeys))
	//state
	distributions[0] = TSD(0, 1, 0)
	//battery_voltage
	distributions[1] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &WindowMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      sensorId,
		windowId:      windowId,
	}
}

func (m *WindowMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *WindowMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(WindowByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	p.AppendTag(WindowTagKey, m.windowId)
	for i := range m.distributions {
		p.AppendField(WindowFieldKeys[i], m.distributions[i].Get())
	}
}
