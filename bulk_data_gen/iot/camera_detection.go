package iot

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	CameraDetectionByteString = []byte("camera_detection") // heap optimization
)

var DetectionObjects = [][]byte{
	[]byte("animal"),
	[]byte("human"),
	[]byte("vehicle"),
	[]byte("unknown"),
}

var Animals = [][]byte{
	[]byte("cat"),
	[]byte("dog"),
	[]byte("bird"),
	[]byte("unknown"),
}

var Humans = [][]byte{
	[]byte("man"),
	[]byte("woman"),
	[]byte("child"),
	[]byte("unknown"),
}

var Vehicles = [][]byte{
	[]byte("car"),
	[]byte("lorry"),
	[]byte("truck"),
	[]byte("motorcycle"),
	[]byte("bicycle"),
	[]byte("unknown"),
}

var (
	// Field keys for 'air condition indoor' points.
	CameraDetectionFieldKeys = [][]byte{
		[]byte("object_type"),
		[]byte("object_kind"),
		[]byte("battery_voltage"),
	}
)

type CameraDetectionMeasurement struct {
	sensorId    []byte
	timestamp   time.Time
	batteryDist Distribution
	object      []byte
	kind        []byte
}

func NewCameraDetectionMeasurement(start time.Time, id []byte) *CameraDetectionMeasurement {

	//battery_voltage
	batteryDist := MUDWD(ND(0.01, 0.005), 1, 3.2, 3.2)

	cd := &CameraDetectionMeasurement{
		timestamp:   start,
		batteryDist: batteryDist,
		sensorId:    id,
	}
	cd.newDetection()
	return cd
}

func (m *CameraDetectionMeasurement) newDetection() {
	object := rand.Int63n(int64(len(DetectionObjects)))
	m.object = DetectionObjects[object]
	switch object {
	case 0: //animal
		m.kind = Animals[rand.Int63n(int64(len(Animals)))]
		break
	case 1: //human
		m.kind = Humans[rand.Int63n(int64(len(Humans)))]
		break
	case 2: //vehicle
		m.kind = Vehicles[rand.Int63n(int64(len(Vehicles)))]
		break
	case 3: //uknown
		m.kind = []byte("uknown")
	}
}

func (m *CameraDetectionMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	m.batteryDist.Advance()
	m.newDetection()
}

func (m *CameraDetectionMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(CameraDetectionByteString)
	p.SetTimestamp(&m.timestamp)
	p.AppendTag(SensorHomeTagKeys[0], m.sensorId)
	p.AppendField(CameraDetectionFieldKeys[0], m.object)
	p.AppendField(CameraDetectionFieldKeys[1], m.kind)
	p.AppendField(CameraDetectionFieldKeys[2], m.batteryDist.Get())
	return true
}
