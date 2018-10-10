package iot

import (
	"fmt"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"time"
)

// Type IotSimulatorConfig is used to create a IotSimulator.
type IotSimulatorConfig struct {
	Start time.Time
	End   time.Time

	SmartHomeCount int64
	SmartHomeOffset int64
}

func (d *IotSimulatorConfig) ToSimulator() *IotSimulator {
	homeInfos := make([]*SmartHome, d.SmartHomeCount)
	var measNum int64

	for i := 0; i < len(homeInfos); i++ {
		homeInfos[i] = NewSmartHome(i, int(d.SmartHomeOffset), d.Start)
		measNum += int64(homeInfos[i].NumMeasurements())
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * measNum
	dg := &IotSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  maxPoints,

		currentHomeIndex: 0,
		homes:            homeInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

// A IotSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type IotSimulator struct {
	madePoints    int64
	maxPoints     int64
	madeValues    int64
	skippedPoints int64

	currentHomeIndex int
	homes            []*SmartHome

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *IotSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *IotSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *IotSimulator) Total() int64 {
	return g.maxPoints
}

func (g *IotSimulator) Finished() bool {
	return (g.madePoints + g.skippedPoints) >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *IotSimulator) Next(p *Point) {
	for {
		//find home which has not send measurement
		homeFound := false
		homesSeen := 0
		for homesSeen < len(g.homes) {
			if g.currentHomeIndex == len(g.homes) {
				g.currentHomeIndex = 0
			}
			if g.homes[g.currentHomeIndex].HasMoreMeasurements() {
				homeFound = true
				break
			}
			g.currentHomeIndex++
			homesSeen++
		}

		if !homeFound {
			for i := 0; i < len(g.homes); i++ {
				g.homes[i].TickAll(EpochDuration)
				g.homes[i].ResetMeasurementCounter()
			}
			g.currentHomeIndex = 0
		}
		sm := g.homes[g.currentHomeIndex].NextMeasurement(p)
		if sm == nil {
			panic(fmt.Sprintf("Null point: home %d, room: %d, home measurement: %d", g.currentHomeIndex, g.homes[g.currentHomeIndex].currentRoomIndex, g.homes[g.currentHomeIndex].currentMeasurement))
		}
		g.currentHomeIndex++

		if !sm.ToPoint(p) {
			p.Reset()
			g.skippedPoints++
			continue
		}
		g.madePoints++
		g.madeValues += int64(len(p.FieldValues))
		break
	}
}
