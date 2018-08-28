package dashboard

import (
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	SystemByteString = []byte("system") // heap optimization
)

var (
	NCPUsFieldKey = []byte("n_cpus")
	// Field keys for 'load' points.
	LoadFieldKeys = [][]byte{
		[]byte("load1"),
		[]byte("load5"),
		[]byte("load15"),
	}

	CPUsCount = []int{2, 4, 8, 16, 32, 64}
)

type SystemMeasurement struct {
	timestamp     time.Time
	ncpus         int
	distributions []Distribution
}

func NewSystemMeasurement(start time.Time) *SystemMeasurement {
	distributions := make([]Distribution, len(LoadFieldKeys))
	ncpus := CPUsCount[rand.Intn(len(CPUsCount))]
	for i := range distributions {
		distributions[i] = &ClampedRandomWalkDistribution{
			State: rand.Float64() * 100.0 * float64(ncpus),
			Min:   0.0,
			Max:   float64(ncpus) * (1 + rand.Float64()),
			Step: &NormalDistribution{
				Mean:   0.0,
				StdDev: 10.0,
			},
		}
	}
	return &SystemMeasurement{
		timestamp:     start,
		ncpus:         ncpus,
		distributions: distributions,
	}
}

func (m *SystemMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *SystemMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(SystemByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendField(NCPUsFieldKey, m.ncpus)
	for i := range m.distributions {
		p.AppendField(LoadFieldKeys[i], m.distributions[i].Get())
	}
	return true
}
