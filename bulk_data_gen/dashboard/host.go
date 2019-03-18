package dashboard

import (
	"fmt"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"
	"math/rand"
	"time"
)

const NHostSims = 11

var ClusterSizes = []int{5, 6, 7, 8, 9, 10, 11, 12, 13}
var ClusterSize = 10

var ClusterIdTagkey = []byte("cluster_id")

// Type Host models a machine being monitored by Telegraf.
type Host struct {
	SimulatedMeasurements []SimulatedMeasurement

	// These are all assigned once, at Host creation:
	Name, Region, Datacenter, Rack, OS, Arch               []byte
	ClusterId, Service, ServiceVersion, ServiceEnvironment []byte
}

func NewHostMeasurements(start time.Time) []SimulatedMeasurement {
	sm := []SimulatedMeasurement{
		devops.NewCPUMeasurement(start),
		devops.NewDiskIOMeasurement(start),
		devops.NewDiskMeasurement(start, 1),
		devops.NewKernelMeasurement(start),
		devops.NewMemMeasurement(start),
		devops.NewNetMeasurement(start),
		devops.NewNginxMeasurement(start),
		devops.NewPostgresqlMeasurement(start),
		devops.NewRedisMeasurement(start),
		NewSystemMeasurement(start),
		NewStatusMeasurement(start),
	}

	if len(sm) != NHostSims {
		panic("logic error: incorrect number of measurements")
	}
	return sm
}

var (
	currentClusterSize int
	clusterId         int
	currentHostIndex  int
)

func NewHost(i int, offset int, start time.Time) Host {
	var hostname []byte
	if i > 0 {
		if currentClusterSize == 0 || currentHostIndex == currentClusterSize {
			currentHostIndex = 0
			currentClusterSize = ClusterSize //ClusterSizes[rand.Intn(len(ClusterSizes))]
			clusterId++
		}

		if currentHostIndex < 3 {
			hostname = []byte(fmt.Sprintf("meta_%d", currentHostIndex+1/*+offset*/)) // hostname is 1-indexed in its cluster
		} else {
			hostname = []byte(fmt.Sprintf("data_%d", currentHostIndex-2/*+offset*/)) // hostname is 1-indexed in its cluster
		}
	} else {
		hostname = []byte(fmt.Sprintf("kapacitor_%d", 1/*+offset*/)) // hostname is 1-indexed in its cluster
	}
	sm := NewHostMeasurements(start)

	region := &devops.Regions[rand.Intn(len(devops.Regions))]
	rackId := rand.Int63n(devops.MachineRackChoicesPerDatacenter)
	serviceId := rand.Int63n(devops.MachineServiceChoices)
	serviceVersionId := rand.Int63n(devops.MachineServiceVersionChoices)
	serviceEnvironment := RandChoice(devops.MachineServiceEnvironmentChoices)
	clusterOffset := offset / ClusterSize

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		Name:               hostname,
		Region:             []byte(fmt.Sprintf("%s", region.Name)),
		Datacenter:         RandChoice(region.Datacenters),
		Rack:               []byte(fmt.Sprintf("%d", rackId)),
		Arch:               RandChoice(devops.MachineArchChoices),
		OS:                 RandChoice(devops.MachineOSChoices),
		Service:            []byte(fmt.Sprintf("%d", serviceId)),
		ServiceVersion:     []byte(fmt.Sprintf("%d", serviceVersionId)),
		ServiceEnvironment: serviceEnvironment,
		ClusterId:          []byte(fmt.Sprintf("%d", clusterId+clusterOffset)),

		SimulatedMeasurements: sm,
	}

	// partial override from external config
	if Config != nil {
		h.Region = Config.GetTagBytesValue(nil, devops.MachineTagKeys[1], true)
		h.Datacenter = Config.GetTagBytesValue(nil, devops.MachineTagKeys[2], true)
		h.Rack = Config.GetTagBytesValue(nil, devops.MachineTagKeys[3], true)
		h.OS = Config.GetTagBytesValue(nil, devops.MachineTagKeys[4], true)
		h.Arch = Config.GetTagBytesValue(nil, devops.MachineTagKeys[5], true)
		h.Service = Config.GetTagBytesValue(nil, devops.MachineTagKeys[7], true)
		h.ServiceVersion = Config.GetTagBytesValue(nil, devops.MachineTagKeys[8], true)
		h.ServiceEnvironment = Config.GetTagBytesValue(nil, devops.MachineTagKeys[9], true)
	}

	currentHostIndex++
	return h
}

// TickAll advances all Distributions of a Host.
func (h *Host) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
}
