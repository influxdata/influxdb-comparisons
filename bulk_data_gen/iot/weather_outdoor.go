package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

var (
	WeatherOutdoorByteString = []byte("weather_outdoor") // heap optimization
)

var (
	// Field keys for 'air condition indoor' points.
	WeatherOutdoorFieldKeys = [][]byte{
		[]byte("pressure"),
		[]byte("wind_speed"),
		[]byte("wind_direction"),
		[]byte("precipitation"),
		[]byte("battery_voltage"),
	}
)

type WeatherOutdoorMeasurement struct {
	sensorId      []byte
	timestamp     time.Time
	distributions []Distribution
}

func NewWeatherOutdoorMeasurement(start time.Time, id []byte) *WeatherOutdoorMeasurement {
	distributions := make([]Distribution, len(WeatherOutdoorFieldKeys))
	//pressure
	distributions[0] = CWD(ND(0, 10), 900, 1200, 1000)
	//wind_speed
	distributions[1] = CWD(ND(0, 1), 0, 60, 0)
	//wind_direction
	distributions[2] = CWD(ND(0, 1), 0, 359, 90)
	//precipitation
	distributions[3] = MUDWD(ND(0, 1), 5, 95, 80)
	//battery_voltage
	distributions[4] = MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	return &WeatherOutdoorMeasurement{
		timestamp:     start,
		distributions: distributions,
		sensorId:      id,
	}
}

func (m *WeatherOutdoorMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *WeatherOutdoorMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(WeatherOutdoorByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	for i := range m.distributions {
		p.AppendField(WeatherOutdoorFieldKeys[i], m.distributions[i].Get())
	}
}
