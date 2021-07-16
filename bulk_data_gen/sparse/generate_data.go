package sparse

import (
	"time"

	"math/rand"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
)

var (
	SpareTagKeys = [][]byte{
		[]byte("X"),
		[]byte("Y"),
	}
	valKey  = []byte("val")
	measKey = []byte("example_measurement")
)

type SparseSimulatorConfig struct {
	Start time.Time
	End   time.Time

	ScaleFactor int64
}

func (d *SparseSimulatorConfig) ToSimulator() *SparseSimulator {
	dg := &SparseSimulator{
		madePoints: 0,
		madeValues: 0,
		maxPoints:  d.ScaleFactor * d.ScaleFactor,

		axis: d.ScaleFactor,
		xIdx: -1,
		yIdx: -1,
		xVal: make([]byte, 10),
		yVal: make([]byte, 10),

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	dg.stepTime = time.Duration(int64(dg.timestampEnd.Sub(dg.timestampStart)) / dg.maxPoints)

	shuffleBytes(dg.xVal)

	return dg
}

type SparseSimulator struct {
	madePoints    int64
	maxPoints     int64
	madeValues    int64
	skippedPoints int64

	axis int64
	xIdx int64
	yIdx int64
	xVal []byte
	yVal []byte

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	stepTime       time.Duration // basically the difference in all the points
}

func (g *SparseSimulator) SeenPoints() int64 {
	return g.madePoints
}

func (g *SparseSimulator) SeenValues() int64 {
	return g.madeValues
}

func (g *SparseSimulator) Total() int64 {
	return g.maxPoints
}

func (g *SparseSimulator) Finished() bool {
	return (g.madePoints + g.skippedPoints) >= g.maxPoints
}

// Next advances a Point to the next state in the generator.
func (g *SparseSimulator) Next(p *common.Point) {
	// always get a new yval
	shuffleBytes(g.yVal)

	// measurement name here i guess
	p.SetMeasurementName(measKey)

	// timestamp
	p.SetTimestamp(&g.timestampNow)

	// Populate tags
	p.AppendTag(SpareTagKeys[0], g.xVal)
	p.AppendTag(SpareTagKeys[1], g.yVal)

	// Populate measurement
	p.AppendField(valKey, rand.Float64())

	// roll the xval into a new one if it's time
	g.xIdx++
	if g.xIdx >= g.axis {
		g.xIdx = 0
		shuffleBytes(g.xVal)
	}

	// this doesn't currently do anything
	g.yIdx++

	g.madePoints++
	g.madeValues += int64(len(p.FieldValues))
	g.timestampNow = g.timestampNow.Add(g.stepTime)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func shuffleBytes(b []byte) {
	for i, cache, remain := len(b)-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
}

// func shuffleBytes2(b []byte) {
// 	for idx := range b {
// 		val := letterBytes[rand.Intn(len(letterBytes))]
// 		b[idx] = val
// 	}
// }
