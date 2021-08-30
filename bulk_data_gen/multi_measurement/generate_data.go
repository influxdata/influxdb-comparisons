package multiMeasurement

import (
	"fmt"
	"time"

	"math/rand"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

const MeasSig = "Measurement-%d"
const FieldSig = "Field-%d"
const NumFields = 1       // number of fields for each measurement
const MeasMultiplier = 50 // scaleVar * measMultiplier = number of unique measurements

type MeasurementSimulatorConfig struct {
	Start time.Time
	End   time.Time

	ScaleFactor int
}

func (d *MeasurementSimulatorConfig) ToSimulator() *MeasurementSimulator {
	s := d.ScaleFactor * MeasMultiplier // number of measurements to create

	dg := &MeasurementSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  int64(s * 50), // 50 points per measurement, or approx. 1 per shard for a year of data

		fieldList: make([][]byte, NumFields),
		measList:  make([][]byte, s),

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	for i := 0; i < s; i++ {
		dg.measList[i] = []byte(fmt.Sprintf(MeasSig, i))
	}

	for i := 0; i < NumFields; i++ {
		dg.fieldList[i] = []byte(fmt.Sprintf(FieldSig, i))
	}

	dg.stepTime = time.Duration(int64(dg.timestampEnd.Sub(dg.timestampStart)) / dg.maxPoints)

	return dg
}

// MeasurementSimulator fullfills the Simulator interface.
type MeasurementSimulator struct {
	madePoints int64
	maxPoints  int64
	madeValues int64

	fieldList [][]byte
	measList  [][]byte

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	stepTime       time.Duration
}

func (g *MeasurementSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *MeasurementSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *MeasurementSimulator) Total() int64 {
	return g.maxPoints
}

func (g *MeasurementSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *MeasurementSimulator) Next(p *common.Point) {
	p.SetMeasurementName(g.measList[rand.Intn(len(g.measList))])
	p.SetTimestamp(&g.timestampNow)

	for _, f := range g.fieldList {
		p.AppendField(f, rand.Float64())
	}

	g.madePoints++
	g.madeValues += int64(len(g.fieldList))
	g.timestampNow = g.timestampNow.Add(g.stepTime)
}
