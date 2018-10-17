package main

import (
	"fmt"
	"github.com/GaryBoone/GoStats/stats"
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

type TimedStatGroup struct {
	maxDuraton time.Duration
	stats      []timedStat
	lastAvg    float64
	lastMedian float64
	trendAvg   *TrendStat
}

func NewTimedStatGroup(maxDuration time.Duration, maxTrendSamples int) *TimedStatGroup {
	return &TimedStatGroup{maxDuraton: maxDuration, stats: make([]timedStat, 0, 100000), trendAvg: NewTrendStat(maxTrendSamples, true)}
}

func (m *TimedStatGroup) Push(timestamp time.Time, value float64) {
	m.stats = append(m.stats, timedStat{timestamp: timestamp, value: value})
}

func (m *TimedStatGroup) Avg() float64 {
	return m.lastAvg
}

func (m *TimedStatGroup) Median() float64 {
	return m.lastMedian
}

func (m *TimedStatGroup) UpdateAvg(now time.Time) (float64, float64) {
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
	m.trendAvg.Add(m.lastAvg)
	return m.lastAvg, m.lastMedian
}

type TrendStat struct {
	x,y	      []float64
	size      int
	slope     float64
	intercept float64
	skipFirst bool
}

func (ls *TrendStat) Add(y float64) float64 {
	c := len(ls.y)
	if c == 0 {
		if ls.skipFirst {
			ls.skipFirst = false
			return 0
		}
	}
	y = y / 1000 // normalize to seconds
	if c < ls.size {
		ls.y = append(ls.y, y)
		c++
		if c < 5 { // at least 5 samples required for regression
			return 0
		}
	} else { // shift left using copy and insert at last position - hopefully no reallocation
		y1 := ls.y[1:]
		copy(ls.y, y1)
		ls.y[ls.size-1] = y
	}
	if c > ls.size {
		panic("Bug in implementation")
	}
	var r stats.Regression
	for i := 0; i < c; i++ {
		r.Update(ls.x[i], ls.y[i] - ls.y[0])
	}
	ls.slope = r.Slope()
	ls.intercept = (r.Intercept() + ls.y[0]) * 1000
	// Y = INTERCEPT + SLOPE * X
	return ls.slope
}

func NewTrendStat(size int, skipFirst bool) *TrendStat  {
	fmt.Printf("Trend statistics using %d samples\n", size)
	instance := TrendStat{
		size:size,
		slope:0,
		skipFirst:skipFirst,
	}
	instance.x = make([]float64, size, size)
	instance.y = make([]float64, 0, size)
	for i := 0; i < size; i++ {
		instance.x[i] = float64(i) // X is constant array { 0, 1, 2 ... size }
	}
	return &instance
}
