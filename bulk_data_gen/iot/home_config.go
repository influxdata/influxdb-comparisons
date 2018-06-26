package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	HomeConfigByteString = []byte("home_config") // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	HomeConfigFieldKeys = [][]byte{
		[]byte("config_string"),
	}
)

type HomeConfigMeasurement struct {
	lastChange time.Time
	sensorId   []byte
	timestamp  time.Time
	config     []byte
}

func NewHomeConfigMeasurement(start time.Time, id []byte) *HomeConfigMeasurement {

	return &HomeConfigMeasurement{
		timestamp:  start,
		lastChange: start,
		sensorId:   id,
		config:     genRandomString(),
	}
}

func (m *HomeConfigMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	//change config only in random 12 hours interval
	if m.timestamp.Sub(m.lastChange) > time.Hour*time.Duration(rand.Int63n(12)+1) {
		m.config = genRandomString()
	}
}

func (m *HomeConfigMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(HomeConfigByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	p.AppendField(HomeConfigFieldKeys[0], m.config)
}

func genRandomString() []byte {
	//len 10-20k
	len := int((rand.Int63n(10) + 10) * 1024)
	buff := make([]byte, len)
	for i := 0; i < len; i++ {
		buff[i] = byte(rand.Int63n(87) + 40)
		for buff[i] == 92 {
			buff[i] = byte(rand.Int63n(87) + 40)
		}
	}
	return buff
}
