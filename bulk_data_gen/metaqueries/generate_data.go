package metaqueries

import (
	"time"

	"math/rand"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var (
	TagKeys = [][]byte{
		[]byte("X"),
		[]byte("Y"),
	}
	valKey  = []byte("val")
	measKey = []byte("example_measurement")
)

type MetaquerySimulatorConfig struct {
	Start time.Time
	End   time.Time

	ScaleFactor int
}

func (d *MetaquerySimulatorConfig) ToSimulator() *MetaquerySimulator {
	dg := &MetaquerySimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  int64(d.ScaleFactor) * int64(d.ScaleFactor),

		axis:    d.ScaleFactor,
		TagList: make(map[int][]byte),

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	// make the tag values to use
	for i := 0; i < d.ScaleFactor; i++ {
		seed := rand.Perm(10)
		thisVal := make([]byte, 10)

		for idx, v := range seed {
			thisVal[idx] = letters[v]
		}

		dg.TagList[i] = thisVal
	}

	dg.stepTime = time.Duration(int64(dg.timestampEnd.Sub(dg.timestampStart)) / dg.maxPoints)

	return dg
}

type MetaquerySimulator struct {
	madePoints    int64
	maxPoints     int64
	madeValues    int64
	skippedPoints int64

	axis    int
	TagList map[int][]byte

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	stepTime       time.Duration // basically the difference in all the points
}

func (g *MetaquerySimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *MetaquerySimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *MetaquerySimulator) Total() int64 {
	return g.maxPoints
}

func (g *MetaquerySimulator) Finished() bool {
	return (g.madePoints + g.skippedPoints) >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *MetaquerySimulator) Next(p *common.Point) {
	p.SetMeasurementName(measKey)
	p.SetTimestamp(&g.timestampNow)

	xIdx := rand.Intn(g.axis)
	yIdx := rand.Intn(g.axis)

	// Populate tags
	p.AppendTag(TagKeys[0], g.TagList[xIdx])
	p.AppendTag(TagKeys[1], g.TagList[yIdx])

	// Populate measurement
	p.AppendField(valKey, rand.Float64())

	g.madePoints++
	g.madeValues += int64(len(p.FieldValues))
	g.timestampNow = g.timestampNow.Add(g.stepTime)
}
