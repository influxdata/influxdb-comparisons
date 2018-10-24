package bulk_query_gen

import (
	"time"
)

var (
	TimeWindowShift time.Duration
	QueryIntervalType string
)

type TimeWindow struct {
	Start time.Time
	Duration time.Duration
}

// Inspired by TimeInterval.RandWindow
func (tw *TimeWindow) SlidingWindow(AllInterval *TimeInterval) TimeInterval {
	start := tw.Start.UnixNano()
	end := tw.Start.Add(tw.Duration).UnixNano()

	if TimeWindowShift > 0 { // shift by user-specified amount
		tw.Start = tw.Start.Add(TimeWindowShift)
	} else { // shift by query duration (default)
		tw.Start = tw.Start.Add(tw.Duration)
	}

	if tw.Start.UnixNano() >= AllInterval.End.UnixNano() {
		tw.Start = AllInterval.Start
	}

	x := NewTimeInterval(time.Unix(0, start), time.Unix(0, end))

	return x
}