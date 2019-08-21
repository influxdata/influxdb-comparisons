package report

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestResultsInfluxDbV1(t *testing.T) {
	var reportTags [][2]string
	reportTags = append(reportTags, [2]string{"hours", "2"})
	reportTags = append(reportTags, [2]string{"hosts", "100"})
	reportParams := &LoadReportParams{
		ReportParams: ReportParams{
			ReportDatabaseName: "test_benchmarks",
			ReportHost:         "http://localhost:8086",
			ReportUser:         "",
			ReportPassword:     "",
			Hostname:           "mypc",
			DBType:             "InfluxDB",
			DestinationUrl:     "http://localhost:8086",
			Workers:            10,
			ItemLimit:          -1,
		},
		IsGzip:    false,
		BatchSize: 5000,
	}
	err := ReportLoadResult(reportParams, 300, 30001000, 23001000, time.Minute*5)
	require.NoError(t, err)
}

func TestResultsInfluxDbV2(t *testing.T) {
	var reportTags [][2]string
	reportTags = append(reportTags, [2]string{"hours", "2"})
	reportTags = append(reportTags, [2]string{"hosts", "100"})
	reportParams := &LoadReportParams{
		ReportParams: ReportParams{
			ReportDatabaseName: "0418a0edc9573000",
			ReportHost:         "http://localhost:9999",
			ReportOrgId:        "03d32366bb107000",
			ReportAuthToken:    "2sRnZBpWjDzF009nGnGHSsmbNMvV36F4GXIvEkNPIH1dTgMiw6G_NmCnfn136w3flrqZ34zv52nb1fB4hiJGCA==",
			Hostname:           "mypc",
			DBType:             "InfluxDB",
			DestinationUrl:     "http://localhost:8086",
			Workers:            10,
			ItemLimit:          -1,
		},
		IsGzip:    false,
		BatchSize: 5000,
	}
	err := ReportLoadResult(reportParams, 300, 30001000, 23001000, time.Minute*5)
	require.NoError(t, err)
}
