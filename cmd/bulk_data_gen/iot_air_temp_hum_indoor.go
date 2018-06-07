package main

import (
	"time"
)

var (
	ATHIByteString      = []byte("air_condition_room")       // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	ATHIFieldKeys = [][]byte{
		[]byte("temperature"),
		[]byte("humidity"),
		[]byte("battery_voltage"),
	}
)

type ATHIMeasurement struct {
	timestamp     time.Time
	distributions []Distribution
}

func NewATHIMeasurement(start time.Time) *ATHIMeasurement {
	distributions := make([]Distribution, len(ATHIFieldKeys))
	//temperature
	distributions[0] = MUDWD(ND(0,1), 15, 28, 15 )
	//humidity
	distributions[1] = MUDWD(ND(0,1), 25, 60, 40 )
	//battery_voltage
	distributions[2] = MUDWD(ND(1,0.5), 1, 3.2, 3.2 )
	return &ATHIMeasurement{
		timestamp:   start,
		distributions: distributions,
	}
}

func (m *ATHIMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *ATHIMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(ATHIByteString)
	p.SetTimestamp(&m.timestamp)

	for i := range m.distributions {
		p.AppendField(ATHIFieldKeys[i], m.distributions[i].Get())
	}
}
