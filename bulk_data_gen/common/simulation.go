package common

import "time"

const (
	DefaultDateTimeStart                = "2018-01-01T00:00:00Z"
	DefaultDateTimeEnd                  = "2018-01-02T00:00:00Z"
	UseCaseDevOps                       = "devops"
	UseCaseIot                          = "iot"
	UseCaseDashboard                    = "dashboard"
	UseCaseMetaquery                    = "metaquery"
	UseCaseWindowAggregate              = "window-agg"
	UseCaseGroupAggregate               = "group-agg"
	UseCaseBareAggregate                = "bare-agg"
	UseCaseGroupWindowTransposeHighCard = "group-window-transpose-high-card"
	UseCaseGroupWindowTransposeLowCard  = "group-window-transpose-low-card"
	UseCaseMultiMeasurement             = "multi-measurement"
)

// Use case choices:
var UseCaseChoices = []string{
	UseCaseDevOps,
	UseCaseIot,
	UseCaseDashboard,
	UseCaseMetaquery,
	UseCaseWindowAggregate,
	UseCaseGroupAggregate,
	UseCaseBareAggregate,
	UseCaseMultiMeasurement,
}

// Simulator simulates a use case.
type Simulator interface {
	Total() int64
	SeenPoints() int64
	SeenValues() int64
	Finished() bool
	Next(*Point)
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*Point) bool //returns true if point if properly filled, false means, that point should be skipped
}

// MakeUsablePoint allocates a new Point ready for use by a Simulator.
func MakeUsablePoint() *Point {
	return &Point{
		MeasurementName: nil,
		TagKeys:         make([][]byte, 0),
		TagValues:       make([][]byte, 0),
		FieldKeys:       make([][]byte, 0),
		FieldValues:     make([]interface{}, 0),
		Timestamp:       &time.Time{},
	}
}
