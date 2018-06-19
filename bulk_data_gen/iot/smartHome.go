package iot
import (
	"fmt"
	"math/rand"
	"time"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

var (
	// The duration of a log epoch.
	IoTEpochDuration = 60 * time.Second

	// Tag fields common to all inside sensors:
	RoomTagKeys = [][]byte{
		[]byte("home_id"),
		[]byte("sensor_id"),
		[]byte("room"),
	}
	// Tag fields common to all inside sensors:
	OutdoorTagKeys = [][]byte{
		[]byte("home_id"),
		[]byte("sensor_id"),
	}
)


type room struct {
	RoomId []byte
	SimulatedMeasurements []SimulatedMeasurement
}

// Type Host models a machine being monitored by Telegraf.
type SmartHome struct {
	SimulatedMeasurements []SimulatedMeasurement
	Rooms []room

	// These are all assigned once, at Host creation:
	HomeId []byte
}

var LastSensorId = 0

func NewSensorId() []byte {
	LastSensorId++
	return []byte(fmt.Sprintf("%013d", LastSensorId))
}

func NewSmartHomeMeasurements(start time.Time) []SimulatedMeasurement {

	rooms := rand.Int63n(6)+6
	sm := []SimulatedMeasurement{
		NewATHIMeasurement(start,NewSensorId()),
	}
	for i:=0;i<int(rooms);i++ {

	}

	//if len(sm) != NHostSims {
	//	panic("logic error: incorrect number of measurements")
	//}
	return sm
}

func NewSmartHome(i int, start time.Time) *SmartHome {
	sm := NewSmartHomeMeasurements(start)

	h := &SmartHome{SimulatedMeasurements: sm }
	//region := &Regions[rand.Intn(len(Regions))]
	//rackId := rand.Int63n(MachineRackChoicesPerDatacenter)
	//serviceId := rand.Int63n(MachineServiceChoices)
	//serviceVersionId := rand.Int63n(MachineServiceVersionChoices)
	//serviceEnvironment := randChoice(MachineServiceEnvironmentChoices)
	//
	//h := devops.Host{
	//	// Tag Values that are static throughout the life of a Host:
	//	Name:               []byte(fmt.Sprintf("host_%d", i)),
	//	Region:             []byte(fmt.Sprintf("%s", region.Name)),
	//	Datacenter:         randChoice(region.Datacenters),
	//	Rack:               []byte(fmt.Sprintf("%d", rackId)),
	//	Arch:               randChoice(MachineArchChoices),
	//	OS:                 randChoice(MachineOSChoices),
	//	Service:            []byte(fmt.Sprintf("%d", serviceId)),
	//	ServiceVersion:     []byte(fmt.Sprintf("%d", serviceVersionId)),
	//	ServiceEnvironment: serviceEnvironment,
	//	Team:               randChoice(MachineTeamChoices),
	//
	//	SimulatedMeasurements: sm,
	//}

	return h
}

// TickAll advances all Distributions of a Host.
func (h *SmartHome) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
}

