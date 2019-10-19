// Tool created to verify bulk_data_gen time series data from pre-specified use cases.
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type MeasurementMetrics struct {
	_measurement    string
	_numOfTagSets   int
	_numOfFieldSets int
}

const lineProtocolMinLen = 50

func main() {
	var measurement string
	var fieldSets string
	//var measuresTagSets []string
	var tagSetParts []string
	var fieldSetParts []string
	var numOfSeries, numOfTagSets, numOfFeildSets, numOfTimestamps = 0, 0, 0, 0
	m := make(map[string]MeasurementMetrics)

	mm := make(map[int]MeasurementMetrics)

	argsWithProg := os.Args

	file, err := os.Open(argsWithProg[1])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())

		line := scanner.Text()
		if len(line) < lineProtocolMinLen {
			continue //skip non line procotol buffers
		}

		// Process text buffer for line protocol metrics.
		// Split the line on spaces.
		parts := strings.Split(line, " ")

		// Loop over the parts from the string.
		for i := range parts {

			switch i {
			case 0:
				// parts[0] --> measurements and tag set(s) [tag sets are optional]
				fmt.Println(parts[i])
				measuresTagSets := parts[i]
				i = strings.Index(parts[i], ",")
				fmt.Println("Index: ", i)
				measurement = measuresTagSets[:i]
				fmt.Println("measurement = ", measurement)
				numOfSeries++

				// Split the line on commas.
				tagSetParts = strings.Split(measuresTagSets, ",")
				fmt.Println("tagSetParts = ", len(tagSetParts))
				numOfTagSets = numOfTagSets + len(tagSetParts)
				fmt.Println("numOfSeries = ", numOfSeries)
				m[measurement] = MeasurementMetrics{
					_measurement: measurement,
				}

				mm[numOfSeries] = MeasurementMetrics{
					_measurement:    measurement,
					_numOfTagSets:   len(tagSetParts),
					_numOfFieldSets: len(fieldSetParts),
				}

			case 1:
				// parts[1] --> field set(s)
				fieldSets = parts[i]
				fmt.Println("fieldSets = ", fieldSets)
				// Split the line on commas.
				fieldSetParts := strings.Split(fieldSets, ",")
				fmt.Println("fieldSetParts = ", len(fieldSetParts))
				i = strings.Index(parts[i], ",")
				fmt.Println("Index: ", i)
				numOfFeildSets = numOfFeildSets + len(fieldSetParts)

				mm[numOfSeries] = MeasurementMetrics{
					_measurement:    measurement,
					_numOfTagSets:   len(tagSetParts),
					_numOfFieldSets: len(fieldSetParts),
				}

			case 2:
				// parts[2] --> timestamp
				timestamp := parts[i]
				fmt.Println("timestamp = ", timestamp)
				numOfTimestamps++
				fmt.Println("numOfTimestamps = ", numOfTimestamps)
			}
		}
	}
	totalTags := 0
	totalFields := 0
	for key := range m {
		totalTags = 0
		totalFields = 0
		for j := 1; j <= len(mm); j++ {
			if m[key]._measurement == mm[j]._measurement {
				totalTags += mm[j]._numOfTagSets
				totalFields += mm[j]._numOfFieldSets
			}
		}
		m[key] = MeasurementMetrics{
			_numOfTagSets:   totalTags,
			_numOfFieldSets: totalFields,
		}
	}
	fmt.Println()
	fmt.Println("----------------------------------------------------------------------------")
	fmt.Println("Line Protocol Workload File: ", argsWithProg[1])
	fmt.Println("----------------------------------------------------------------------------")
	fmt.Println("Total number of measurements entries (series/points): ", numOfSeries)
	fmt.Println("Total catagories of measurements: ", len(mm)) //measurements map length
	for key, val := range m {                                  // not using element (value), just printing keys
		fmt.Println("	Measurement Name: ", key, "			#Tags: ", val._numOfTagSets, "			#Fields ", val._numOfFieldSets)
	}
	fmt.Println()

	fmt.Println("Total number of tag sets: ", numOfTagSets)
	fmt.Println()

	fmt.Println("Total number of field sets: ", numOfFeildSets)
	fmt.Println()

	fmt.Println("Total number of series Timestamps: ", numOfTimestamps)

	fmt.Println()
	fmt.Println()
	fmt.Println("Total elements in the data set (numOfSeries + numOfTagSets + numOfFeildSets): ", numOfSeries+numOfTagSets+numOfFeildSets)
	fmt.Println("----------------------------------------------------------------------------")
	//panic(0)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
