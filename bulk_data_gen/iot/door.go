package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	DoorByteString = []byte("door_state") // heap optimization
	DoorTagKey     = []byte("door_id")
)

var (
	// Field keys for 'air condition indoor' points.
	DoorFieldKeys = [][]byte{
		[]byte("state"),
		[]byte("battery_voltage"),
	}
)

type DoorMeasurement struct {
	sensorId      []byte
	doorId        []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewDoorMeasurement(start time.Time, doorId []byte, sendorId []byte) *DoorMeasurement {
	distributions := make([]Distribution, len(DoorFieldKeys))
	//state
	distributions[0] = TSD(0, 1, 0)
	//battery_voltage
	distributions[1] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &DoorMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      sendorId,
		doorId:        doorId,
	}
}

func (m *DoorMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *DoorMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(DoorByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(DoorTagKey, m.doorId)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(DoorFieldKeys[i], m.distributions[i].Get())
	}
}
