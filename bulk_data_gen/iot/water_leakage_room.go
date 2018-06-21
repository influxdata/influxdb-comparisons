package iot

import (
	"time"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

var (
	WaterLeakageRoomByteString      = []byte("water_leakage_room")       // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	WaterLeakageRoomFieldKeys = [][]byte{
		[]byte("leakage"),
		[]byte("battery_voltage"),

	}
)

type WaterLeakageRoomMeasurement struct {
	sensorId 	[]byte
	timestamp     time.Time
	distributions []Distribution
}

func NewWaterLeakageRoomMeasurement(start time.Time, id []byte) *WaterLeakageRoomMeasurement {
	distributions := make([]Distribution, len(WaterLeakageRoomFieldKeys))
	//state
	distributions[0] = TSD(0,1,0)
	//battery_voltage
	distributions[1] = MUDWD(ND(1,0.5), 1, 3.2, 3.2 )

	return &WaterLeakageRoomMeasurement{
		timestamp:   start,
		distributions: distributions,
		sensorId: id,
	}
}

func (m *WaterLeakageRoomMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *WaterLeakageRoomMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(WaterLeakageRoomByteString)
	p.SetTimestamp(&m.timestamp)

	for i := range m.distributions {
		p.AppendField(WaterLeakageRoomFieldKeys[i], m.distributions[i].Get())
	}
}
