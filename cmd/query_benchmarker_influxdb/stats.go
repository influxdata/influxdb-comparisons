package main

import (
	"fmt"
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
}

func NewTimedStatGroup(maxDuration time.Duration) *TimedStatGroup {
	return &TimedStatGroup{maxDuraton: maxDuration, stats: make([]timedStat, 0, 100000)}
}

func (m *TimedStatGroup) Push(timestamp time.Time, value float64) {
	m.stats = append(m.stats, timedStat{timestamp: timestamp, value: value})
}

func (m *TimedStatGroup) Avg() float64 {
	return m.lastAvg
}

func (m *TimedStatGroup) UpdateAvg() float64 {
	newStats := make([]timedStat, 0, len(m.stats))
	last := time.Now().Add(-m.maxDuraton)
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

	m.lastAvg = sum / float64(c)
	return m.lastAvg
}
