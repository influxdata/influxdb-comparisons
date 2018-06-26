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
}

func (d *IotSimulatorConfig) ToSimulator() *IotSimulator {
	homeInfos := make([]*SmartHome, d.SmartHomeCount)
	var measNum int64

	for i := 0; i < len(homeInfos); i++ {
		homeInfos[i] = NewSmartHome(i, d.Start)
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
	madePoints int64
	maxPoints  int64
	madeValues int64

	currentHomeIndex int
	homes            []*SmartHome

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *IotSimulator) Seen() int64 {
	return g.madePoints
}

func (g *IotSimulator) Total() int64 {
	return g.maxPoints
}

func (g *IotSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (d *IotSimulator) Next(p *Point) {

	//find home which has not send measurement
	homeFound := false
	homesSeen := 0
	for homesSeen < len(d.homes) {
		if d.currentHomeIndex == len(d.homes) {
			d.currentHomeIndex = 0
		}
		if d.homes[d.currentHomeIndex].HasMoreMeasurements() {
			homeFound = true
			break
		}
		d.currentHomeIndex++
		homesSeen++
	}

	if !homeFound {
		for i := 0; i < len(d.homes); i++ {
			d.homes[i].TickAll(EpochDuration)
			d.homes[i].ResetMeasurementCounter()
		}
		d.currentHomeIndex = 0
	}
	sm := d.homes[d.currentHomeIndex].NextMeasurementToPoint(p)
	if sm != nil {
		d.madePoints++
		d.currentHomeIndex++
		d.madeValues += int64(len(p.FieldValues))
	} else {
		panic(fmt.Sprintf("Null point: home %d, room: %d, home measurement: %d", d.currentHomeIndex, d.homes[d.currentHomeIndex].currentRoom, d.homes[d.currentHomeIndex].currentMeasurement))
	}
}
