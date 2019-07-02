package bulk_load

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Stat represents one statistical measurement.
type Stat struct {
	Label []byte
	Value float64
}

// Init safely initializes a stat while minimizing heap allocations.
func (s *Stat) Init(label []byte, value float64) {
	s.Label = s.Label[:0] // clear
	s.Label = append(s.Label, label...)
	s.Value = value
}

// StatGroup collects simple streaming statistics.
type StatGroup struct {
	Min  float64
	Max  float64
	Mean float64
	Sum  float64

	Count int64
}

// Push updates a StatGroup with a new value.
func (s *StatGroup) Push(n float64) {
	if s.Count == 0 {
		s.Min = n
		s.Max = n
		s.Mean = n
		s.Count = 1
		s.Sum = n
		return
	}

	if n < s.Min {
		s.Min = n
	}
	if n > s.Max {
		s.Max = n
	}

	s.Sum += n

	// constant-space mean update:
	sum := s.Mean*float64(s.Count) + n
	s.Mean = sum / float64(s.Count+1)

	s.Count++
}

// String makes a simple description of a StatGroup.
func (s *StatGroup) String() string {
	return fmt.Sprintf("min: %f, max: %f, mean: %f, count: %d, sum: %f", s.Min, s.Max, s.Mean, s.Count, s.Sum)
}

type timedStat struct {
	timestamp time.Time
	value     float64
}

type HistoryItem struct {
	value float64
	item  int
}

type TimedStatGroup struct {
	maxDuraton  time.Duration
	stats       []timedStat
	lastAvg     float64
	lastMedian  float64
	lastRate    float64
	trendAvg    *TrendStat
	statHistory []*HistoryItem
}

func NewTimedStatGroup(maxDuration time.Duration, maxTrendSamples int) *TimedStatGroup {
	return &TimedStatGroup{maxDuraton: maxDuration, stats: make([]timedStat, 0, 100000), trendAvg: NewTrendStat(maxTrendSamples, true), statHistory: make([]*HistoryItem, 0, 512)}
}

func (m *TimedStatGroup) Push(timestamp time.Time, value float64) {
	m.stats = append(m.stats, timedStat{timestamp: timestamp, value: value})
}

func (m *TimedStatGroup) Avg() float64 {
	return m.lastAvg
}

func (m *TimedStatGroup) Rate() float64 {
	return m.lastRate
}

func (m *TimedStatGroup) Median() float64 {
	return m.lastMedian
}

func (m *TimedStatGroup) UpdateAvg(now time.Time, workers int) (float64, float64) {
	newStats := make([]timedStat, 0, len(m.stats))
	last := now.Add(-m.maxDuraton)
	sum := float64(0)
	c := 0

	for _, ts := range m.stats {
		if ts.timestamp.After(last) {
			sum += ts.value
			c++
			newStats = append(newStats, ts)
		}
	}
	m.stats = nil
	m.stats = newStats

	l := len(newStats)
	if l == 0 {
		m.lastMedian = math.NaN()
	} else {
		sort.Slice(newStats, func(i, j int) bool {
			return newStats[i].value < newStats[j].value
		})
		m.lastMedian = newStats[l/2].value
	}

	m.lastAvg = sum / float64(c)
	m.lastRate = sum / m.maxDuraton.Seconds()
	m.statHistory = append(m.statHistory, &HistoryItem{m.lastRate, workers})
	m.trendAvg.Add(m.lastAvg)
	return m.lastAvg, m.lastMedian
}

type TrendStat struct {
	x, y      []float64
	size      int
	slope     float64
	intercept float64
	skipFirst bool
}

func (ls *TrendStat) Add(y float64) {
	c := len(ls.y)
	if c == 0 {
		if ls.skipFirst {
			ls.skipFirst = false
			return
		}
	}
	y = y / 1000 // normalize to seconds
	if c < ls.size {
		ls.y = append(ls.y, y)
		c++
		if c < 5 { // at least 5 samples required for regression
			return
		}
	} else { // shift left using copy and insert at last position - hopefully no reallocation
		y1 := ls.y[1:]
		copy(ls.y, y1)
		ls.y[ls.size-1] = y
	}
	if c > ls.size {
		panic("Bug in implementation")
	}
	//var r stats.Regression
	var r SimpleRegression
	r.hasIntercept = false
	for i := 0; i < c; i++ {
		r.Update(ls.x[i], ls.y[i]-ls.y[0])
	}
	ls.slope = r.Slope()
	ls.intercept = (r.Intercept() + ls.y[0]) * 1000
}

func NewTrendStat(size int, skipFirst bool) *TrendStat {
	fmt.Printf("Trend statistics using %d samples\n", size)
	instance := TrendStat{
		size:      size,
		slope:     0,
		skipFirst: skipFirst,
	}
	instance.x = make([]float64, size, size)
	instance.y = make([]float64, 0, size)
	for i := 0; i < size; i++ {
		instance.x[i] = float64(i) // X is constant array { 0, 1, 2 ... size }
	}
	return &instance
}

type SimpleRegression struct {
	sumX  float64
	sumXX float64
	sumY  float64
	sumYY float64
	sumXY float64

	n float64

	xbar float64
	ybar float64

	hasIntercept bool
}

func (sr *SimpleRegression) Update(x, y float64) {
	if sr.n == 0 {
		sr.xbar = x
		sr.ybar = y
	} else {
		if sr.hasIntercept {
			fact1 := 1.0 + sr.n
			fact2 := sr.n / (1.0 + sr.n)
			dx := x - sr.xbar
			dy := y - sr.ybar
			sr.sumXX += dx * dx * fact2
			sr.sumYY += dy * dy * fact2
			sr.sumXY += dx * dy * fact2
			sr.xbar += dx / fact1
			sr.ybar += dy / fact1
		}
	}
	if !sr.hasIntercept {
		sr.sumXX += x * x
		sr.sumYY += y * y
		sr.sumXY += x * y
	}
	sr.sumX += x
	sr.sumY += y
	sr.n++
}

func (sr *SimpleRegression) Intercept() float64 {
	if sr.hasIntercept {
		return (sr.sumY - sr.Slope()*sr.sumX) / sr.n
	} else {
		return 0
	}
}

func (sr *SimpleRegression) Slope() float64 {
	if sr.n < 2 {
		return math.NaN()
	}
	return sr.sumXY / sr.sumXX
}
