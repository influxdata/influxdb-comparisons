package dashboard

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"
	"time"
)

// A DashboardSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type DashboardSimulator struct {
	madePoints int64
	madeValues int64
	maxPoints  int64

	simulatedMeasurementIndex int

	hostIndex int
	hosts     []Host

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *DashboardSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *DashboardSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *DashboardSimulator) Total() int64 {
	return g.maxPoints
}

func (g *DashboardSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Type DashboardSimulatorConfig is used to create a DashboardSimulator.
type DashboardSimulatorConfig struct {
	Start time.Time
	End   time.Time

	HostCount int64
	HostOffset int64
}

func (d *DashboardSimulatorConfig) ToSimulator() *DashboardSimulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = NewHost(i, int(d.HostOffset), d.Start)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / devops.EpochDuration.Nanoseconds()
	maxPoints := epochs * (d.HostCount * NHostSims)
	dg := &DashboardSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		simulatedMeasurementIndex: 0,

		hostIndex: 0,
		hosts:     hostInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// Next advances a Point to the next state in the generator.
func (d *DashboardSimulator) Next(p *Point) {
	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0
		d.simulatedMeasurementIndex++
	}

	if d.simulatedMeasurementIndex == NHostSims {
		d.simulatedMeasurementIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(devops.EpochDuration)
		}
	}

	host := &d.hosts[d.hostIndex]

	// Populate host-specific tags:
	p.AppendTag(devops.MachineTagKeys[0], host.Name)
	p.AppendTag(devops.MachineTagKeys[1], host.Region)
	p.AppendTag(devops.MachineTagKeys[2], host.Datacenter)
	p.AppendTag(devops.MachineTagKeys[3], host.Rack)
	p.AppendTag(devops.MachineTagKeys[4], host.OS)
	p.AppendTag(devops.MachineTagKeys[5], host.Arch)
	p.AppendTag(ClusterIdTagkey, host.ClusterId)
	p.AppendTag(devops.MachineTagKeys[7], host.Service)
	p.AppendTag(devops.MachineTagKeys[8], host.ServiceVersion)
	p.AppendTag(devops.MachineTagKeys[9], host.ServiceEnvironment)

	// Populate measurement-specific tags and fields:
	host.SimulatedMeasurements[d.simulatedMeasurementIndex].ToPoint(p)

	d.madePoints++
	d.hostIndex++
	d.madeValues += int64(len(p.FieldValues))

	return
}
