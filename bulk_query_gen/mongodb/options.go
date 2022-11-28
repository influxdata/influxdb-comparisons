package mongodb

import (
	"log"
	"strings"
)

const (
	FlatFormat = "flat"
	KeyPairFormat = "key-pair"
	TimeseriesFormat = "timeseries"
)

var DocumentFormat = FlatFormat
var UseTimeseries = false
var UseSingleCollection = false

func ParseOptions(documentFormat string, oneCollection bool) {
	switch documentFormat {
	case FlatFormat, KeyPairFormat, TimeseriesFormat:
		DocumentFormat = documentFormat
	default:
		log.Fatalf("unsupported document format: '%s'", documentFormat)
	}
	UseTimeseries = strings.Contains(documentFormat, TimeseriesFormat)
	if UseTimeseries {
		log.Print("Using MongoDB 5+ time series collection")
		DocumentFormat = FlatFormat
	}
	log.Printf("Using %s point serialization", DocumentFormat)
	UseSingleCollection = oneCollection
	if UseSingleCollection {
		log.Println("Using single collection for all measurements")
	} else {
		log.Println("Using collections per measurement type")
	}
}
