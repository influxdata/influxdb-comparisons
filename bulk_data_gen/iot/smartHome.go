package iot

import (
	"fmt"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	// The duration of a log epoch.
	EpochDuration = 60 * time.Second

	// Tag fields common to all inside sensors:
	RoomTagKey = []byte("room_id")

	// Tag fields common to all inside sensors:
	SensorHomeTagKeys = [][]byte{
		[]byte("sensor_id"),
		[]byte("home_id"),
	}
)

type room struct {
	RoomId                []byte
	SimulatedMeasurements []SimulatedMeasurement
	currentMeasurement    int
}

// Type Host models a machine being monitored by Telegraf.
type SmartHome struct {
	// These are all assigned once, at Host creation:
	SimulatedMeasurements []SimulatedMeasurement
	Rooms                 []*room
	HomeId                []byte
	//cached value of total measurements in home
	measurementsNum int
	//last generated room id
	lastRoomId int64
	//point generation variables
	currentRoomIndex       int
	currentMeasurement     int
	totalMeasurementsGiven int
}

var LastSensorId = 0

func NewSensorId() []byte {
	LastSensorId++
	return []byte(fmt.Sprintf("%013d", LastSensorId))
}

const SmartHomeIdFormat = "%013d"

func NewSmartHome(id int, offset int, start time.Time) *SmartHome {
	h := &SmartHome{HomeId: []byte(fmt.Sprintf(SmartHomeIdFormat, id+offset))}
	h.NewSmartHomeMeasurements(start)
	return h
}

// TickAll advances all Distributions of a Host.
func (r *room) TickAll(d time.Duration) {
	for _, m := range r.SimulatedMeasurements {
		m.Tick(d)
	}
}

func (r *room) ResetMeasurementCounter() {
	r.currentMeasurement = 0
}

func (r *room) HasMoreMeasurements() bool {
	return r.currentMeasurement < len(r.SimulatedMeasurements)
}

func (r *room) NextMeasurement(p *Point) SimulatedMeasurement {
	if !r.HasMoreMeasurements() {
		return nil
	}
	p.AppendTag(RoomTagKey, r.RoomId)
	sm := r.SimulatedMeasurements[r.currentMeasurement]
	r.currentMeasurement++
	return sm
}

func (h *SmartHome) NumMeasurements() int {
	if h.measurementsNum == 0 {
		for _, room := range h.Rooms {
			h.measurementsNum += len(room.SimulatedMeasurements)
		}
		h.measurementsNum += len(h.SimulatedMeasurements)
	}
	return h.measurementsNum
}

func (h *SmartHome) NewRoom(id int, start time.Time) *room {
	h.lastRoomId++
	windowsNum := int(rand.Int63n(3) + 1)
	sm := make([]SimulatedMeasurement, 0, windowsNum*2+3)
	for w := 0; w < windowsNum; w++ {
		sm = append(sm, NewWindowMeasurement(start, []byte(fmt.Sprintf("%d", w+1)), NewSensorId()),
			NewRadiatorValveRoomMeasurement(start, []byte(fmt.Sprintf("%d", w+1)), NewSensorId()))
	}
	sm = append(sm, NewAirConditionRoomMeasurement(start, NewSensorId()))
	sm = append(sm, NewAirQualityRoomMeasurement(start, NewSensorId()))
	sm = append(sm, NewLightLevelRoomMeasurement(start, NewSensorId()))

	return &room{RoomId: []byte(fmt.Sprintf("%d", h.lastRoomId)), SimulatedMeasurements: sm}
}

func (h *SmartHome) NewSmartHomeMeasurements(start time.Time) {

	roomsNum := rand.Int63n(6) + 4
	h.Rooms = make([]*room, roomsNum)
	for i := 0; i < int(roomsNum); i++ {
		h.Rooms[i] = h.NewRoom(i+1, start)
	}
	doorsNum := rand.Int63n(3) + 1

	h.SimulatedMeasurements = []SimulatedMeasurement{
		NewAirConditionOutdoorMeasurement(start, NewSensorId()),
		NewWeatherOutdoorMeasurement(start, NewSensorId()),
		NewHomeStateMeasurement(start, NewSensorId()),
		NewHomeConfigMeasurement(start, NewSensorId()),
		NewCameraDetectionMeasurement(start, NewSensorId()),
		NewWaterLevelMeasurement(start, NewSensorId()),
		NewWaterLeakageRoomMeasurement(start, []byte(fmt.Sprintf("%d", rand.Int63n(roomsNum)+1)), NewSensorId()),
		NewWaterLeakageRoomMeasurement(start, []byte(fmt.Sprintf("%d", rand.Int63n(roomsNum)+1)), NewSensorId()),
	}
	for i := 0; i < int(doorsNum); i++ {
		h.SimulatedMeasurements = append(h.SimulatedMeasurements, NewDoorMeasurement(start, []byte(fmt.Sprintf("%d", i)), NewSensorId()))
	}
}

// TickAll advances all Distributions of a Host.
func (h *SmartHome) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
	for _, room := range h.Rooms {
		room.TickAll(d)
	}
}

func (h *SmartHome) ResetMeasurementCounter() {
	h.totalMeasurementsGiven = 0
	h.currentMeasurement = 0
	h.currentRoomIndex = 0
	for _, room := range h.Rooms {
		room.ResetMeasurementCounter()
	}
}

func (h *SmartHome) HasMoreMeasurements() bool {
	return h.totalMeasurementsGiven < h.NumMeasurements()
}

func (h *SmartHome) NextMeasurement(p *Point) SimulatedMeasurement {
	var sm SimulatedMeasurement
	if !h.HasMoreMeasurements() {
		return nil
	}
	for h.currentRoomIndex < len(h.Rooms) {
		r := h.Rooms[h.currentRoomIndex]
		if r.HasMoreMeasurements() {
			sm = r.NextMeasurement(p)
			break
		} else {
			h.currentRoomIndex++
		}

	}
	if h.currentRoomIndex == len(h.Rooms) {
		sm = h.SimulatedMeasurements[h.currentMeasurement]
		h.currentMeasurement++
	}
	if sm != nil {
		p.AppendTag(SensorHomeTagKeys[1], h.HomeId)
		h.totalMeasurementsGiven++
	}
	return sm
}
