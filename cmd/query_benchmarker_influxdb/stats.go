package main

import (
	"errors"
	"fmt"
	"log"
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
	trendAvg    *TrendStat
	statHistory []*HistoryItem
}

func NewTimedStatGroup(maxDuration time.Duration) *TimedStatGroup {
	return &TimedStatGroup{maxDuraton: maxDuration, stats: make([]timedStat, 0, 100000), trendAvg: NewTrendStat(true), statHistory: make([]*HistoryItem, 0, 512)}
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
	m.statHistory = append(m.statHistory, &HistoryItem{m.lastAvg, workers})
	if responseTimeLimit > 0 {
		m.trendAvg.Add(m.lastAvg, now)
	}
	return m.lastAvg, m.lastMedian
}

func (m *TimedStatGroup) FindHistoryItemBelow(val float64) *HistoryItem {
	item := -1
	for i := len(m.statHistory) - 1; i >= 0; i-- {
		if m.statHistory[i].value < val {
			item = i + 1
			if item == len(m.statHistory) {
				log.Printf("FindHistoryItemBelow: Adjusting returned value from %d to %d\n", item, i)
				item = i
			}
			break
		}
	}
	if item > -1 {
		return m.statHistory[item]
	}
	return nil
}

type TrendStat struct {
	x, y      []float64
	size      int
	slope     float64
	intercept float64
	skipFirst bool
	r         *SimpleRegression
	w         []int
	t0        time.Time
}

func (ls *TrendStat) Add(y float64, now time.Time) {
	c := len(ls.y)
	if c == 0 {
		if ls.skipFirst {
			ls.skipFirst = false
			return
		}
		ls.t0 = now
	}
	y = y / 1000 // normalize to seconds
	if ls.size < 0 { // unlimited history
		ls.y = append(ls.y, y)
		ls.x = append(ls.x, now.Sub(ls.t0).Seconds())
		ls.w = append(ls.w, workers)
		c++
	} else { // limited history
		if c < ls.size {
			ls.y = append(ls.y, y)
			c++
		} else { // shift left using copy and insert at last position - hopefully no reallocation
			y1 := ls.y[1:]
			copy(ls.y, y1)
			ls.y[ls.size - 1] = y
		}
		if c > ls.size {
			panic("Bug in implementation")
		}
	}
	if ls.size < 0 {
		ls.r.Update(float64(c - 1), y)
		ls.slope = ls.r.Slope() * 1000
		ls.intercept = ls.r.Intercept() * 1000
	} else {
		r := &SimpleRegression{
			hasIntercept:false,
		}
		for i := 0; i < c; i++ {
			r.Update(ls.x[i], ls.y[i] - ls.y[0])
		}
		ls.slope = r.Slope() * 1000
		ls.intercept = (r.Intercept() + ls.y[0]) * 1000
	}
}

func NewTrendStat(skipFirst bool) *TrendStat  {
	fmt.Printf("Trend statistics using %d samples (-1 means unlimited), is active? %v\n", trendSamples, responseTimeLimit > 0)
	instance := TrendStat{
		size:trendSamples,
		skipFirst:skipFirst,
	}
	if instance.size < 0 {
		instance.x = make([]float64, 0, 1024)
		instance.y = make([]float64, 0, 1024)
		instance.w = make([]int, 0, 1024)
		instance.r = &SimpleRegression{
			hasIntercept:true,
		}
	} else {
		instance.x = make([]float64, instance.size, instance.size)
		instance.y = make([]float64, 0, instance.size)
		instance.w = nil
		for i := 0; i < instance.size; i++ {
			instance.x[i] = float64(i) // X is constant array { 0, 1, 2 ... size }
		}
	}
	return &instance
}

// Finds number of workers for given response time in ms.
func (ls *TrendStat) NumWorkersByResponseTime(y float64) (int, error) {
	if ls.w != nil {
		x := (y - ls.intercept) / (ls.slope * 1000) // y and intercept in ms
		//log.Printf("Get num of workers for %v ms at %v (%v after %v)\n", y, x, ls.t0.Add(ls.xToDuration(x)), ls.t0)
		n,err := ls.NumWorkersAtTime(ls.t0.Add(ls.xToDuration(x)))
		if err == nil {
			return n, nil
		}
		return -1, err
	}
	return -1, errors.New("workers history unavailable in limited samples mode")
}

// Finds number of workers at given time.
func (ls *TrendStat) NumWorkersAtTime(t time.Time) (int, error) {
	if ls.w != nil {
		if t.Before(ls.t0) || t.After(ls.t0.Add(ls.xToDuration(ls.x[len(ls.x) - 1]))) {
			return -1, errors.New("calculated time out of bounds")
		}
		for i := 1 ; i < len(ls.x); i++ {
			//fmt.Printf("%v after %v (%d: %v)?\n", t, ls.t0.Add(ls.xToDuration(ls.x[i])), i, ls.x[i])
			if t.Before(ls.t0.Add(ls.xToDuration(ls.x[i]))) {
				//fmt.Printf("Found x (ie. time) slot at position %d out of %d\n", i, len(ls.x))
				return ls.w[i], nil
			}
		}
		return -1, errors.New("implementation bug: no time slot found despite successful boundary check")
	}
	return -1, errors.New("workers history unavailable in limited samples mode")
}

func (ls *TrendStat) xToDuration(x float64) time.Duration { // x is in seconds
	return time.Duration(int64(x * 1000)) * time.Millisecond
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
