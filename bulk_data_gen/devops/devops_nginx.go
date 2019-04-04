package devops

import (
	"fmt"
	. "github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"math/rand"
	"time"
)

var (
	NginxByteString = []byte("nginx") // heap optimization

	NginxTags = [][]byte{
		[]byte("port"),
		[]byte("server"),
	}

	NginxFields = []LabeledDistributionMaker{
		{[]byte("accepts"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("active"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("handled"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("reading"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("requests"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("waiting"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("writing"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
	}
)

type NginxMeasurement struct {
	timestamp time.Time

	port, serverName []byte
	distributions    []Distribution
}

func NewNginxMeasurement(start time.Time) *NginxMeasurement {
	distributions := make([]Distribution, len(NginxFields))
	for i := range NginxFields {
		distributions[i] = NginxFields[i].DistributionMaker()
	}

	serverName := []byte(fmt.Sprintf("nginx_%d", rand.Intn(100000)))
	port := []byte(fmt.Sprintf("%d", rand.Intn(20000)+1024))
	if Config != nil { // partial override from external config
		serverName = Config.GetTagBytesValue(NginxByteString, NginxTags[1], true, serverName)
		port = Config.GetTagBytesValue(NginxByteString, NginxTags[0], true, port)
	}
	return &NginxMeasurement{
		port:       port,
		serverName: serverName,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *NginxMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *NginxMeasurement) ToPoint(p *Point) bool {
	p.SetMeasurementName(NginxByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(NginxTags[0], m.port)
	p.AppendTag(NginxTags[1], m.serverName)

	for i := range m.distributions {
		p.AppendField(NginxFields[i].Label, int64(m.distributions[i].Get()))
	}
	return true
}
