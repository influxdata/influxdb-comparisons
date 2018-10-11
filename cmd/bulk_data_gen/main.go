// bulk_data_gen generates time series data from pre-specified use cases.
//
// Supported formats:
// InfluxDB bulk load format
// ElasticSearch bulk load format
// Cassandra query format
// Mongo custom format
// OpenTSDB bulk HTTP format
//
// Supported use cases:
// Devops: scale_var is the number of hosts to simulate, with log messages
//         every 10 seconds.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/dashboard"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/iot"
	"log"
	"os"
	"strings"
	"time"
)

// Output data format choices:
var formatChoices = []string{"influx-bulk", "es-bulk", "cassandra", "mongo", "opentsdb", "timescaledb-sql", "timescaledb-copyFrom"}

// Use case choices:
var useCaseChoices = []string{"devops", "iot", "dashboard"}

// Program option vars:
var (
	daemonUrl string
	dbName    string

	format  string
	useCase string

	scaleVar int64
	scaleVarOffset int64

	timestampStartStr string
	timestampEndStr   string

	timestampStart time.Time
	timestampEnd   time.Time

	interleavedGenerationGroupID uint
	interleavedGenerationGroups  uint

	seed  int64
	debug int
)

// Parse args:
func init() {
	flag.StringVar(&format, "format", formatChoices[0], fmt.Sprintf("Format to emit. (choices: %s)", strings.Join(formatChoices, ", ")))

	flag.StringVar(&useCase, "use-case", useCaseChoices[0], fmt.Sprintf("Use case to model. (choices: %s)", strings.Join(useCaseChoices, ", ")))
	flag.Int64Var(&scaleVar, "scale-var", 1, "Scaling variable specific to the use case.")
	flag.Int64Var(&scaleVarOffset, "scale-var-offset", 0, "Scaling variable offset specific to the use case.")

	flag.StringVar(&timestampStartStr, "timestamp-start", common.DefaultDateTimeStart, "Beginning timestamp (RFC3339).")
	flag.StringVar(&timestampEndStr, "timestamp-end", common.DefaultDateTimeEnd, "Ending timestamp (RFC3339).")

	flag.Int64Var(&seed, "seed", 0, "PRNG seed (default, or 0, uses the current timestamp).")
	flag.IntVar(&debug, "debug", 0, "Debug printing (choices: 0, 1, 2) (default 0).")

	flag.UintVar(&interleavedGenerationGroupID, "interleaved-generation-group-id", 0, "Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.")
	flag.UintVar(&interleavedGenerationGroups, "interleaved-generation-groups", 1, "The number of round-robin serialization groups. Use this to scale up data generation to multiple processes.")

	flag.Parse()

	if !(interleavedGenerationGroupID < interleavedGenerationGroups) {
		log.Fatal("incorrect interleaved groups configuration")
	}

	validFormat := false
	for _, s := range formatChoices {
		if s == format {
			validFormat = true
			break
		}
	}
	if !validFormat {
		log.Fatal("invalid format specifier")
	}

	// the default seed is the current timestamp:
	if seed == 0 {
		seed = int64(time.Now().Nanosecond())
	}
	fmt.Fprintf(os.Stderr, "using random seed %d\n", seed)

	// Parse timestamps:
	var err error
	timestampStart, err = time.Parse(time.RFC3339, timestampStartStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampStart = timestampStart.UTC()
	timestampEnd, err = time.Parse(time.RFC3339, timestampEndStr)
	if err != nil {
		log.Fatal(err)
	}
	timestampEnd = timestampEnd.UTC()
}

func main() {
	common.Seed(seed)

	out := bufio.NewWriterSize(os.Stdout, 4<<20)
	defer out.Flush()

	var sim common.Simulator

	switch useCase {
	case useCaseChoices[0]:
		cfg := &devops.DevopsSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			HostCount: scaleVar,
			HostOffset: scaleVarOffset,
		}
		sim = cfg.ToSimulator()
	case useCaseChoices[2]:
		cfg := &dashboard.DashboardSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			HostCount: scaleVar,
			HostOffset: scaleVarOffset,
		}
		sim = cfg.ToSimulator()
	case useCaseChoices[1]:
		cfg := &iot.IotSimulatorConfig{
			Start: timestampStart,
			End:   timestampEnd,

			SmartHomeCount: scaleVar,
			SmartHomeOffset: scaleVarOffset,
		}
		sim = cfg.ToSimulator()
	default:
		panic("unreachable")
	}

	var serializer common.Serializer
	switch format {
	case "influx-bulk":
		serializer = common.NewSerializerInflux()
	case "es-bulk":
		serializer = common.NewSerializerElastic()
	case "cassandra":
		serializer = common.NewSerializerCassandra()
	case "mongo":
		serializer = common.NewSerializerMongo()
	case "opentsdb":
		serializer = common.NewSerializerOpenTSDB()
	case "timescaledb-sql":
		serializer = common.NewSerializerTimescaleSql()
	case "timescaledb-copyFrom":
		serializer = common.NewSerializerTimescaleBin()
	default:
		panic("unreachable")
	}

	var currentInterleavedGroup uint = 0

	t := time.Now()
	point := common.MakeUsablePoint()
	n := int64(0)
	for !sim.Finished() {
		sim.Next(point)
		n++
		// in the default case this is always true
		if currentInterleavedGroup == interleavedGenerationGroupID {
			//println("printing")
			err := serializer.SerializePoint(out, point)
			if err != nil {
				log.Fatal(err)
			}

		}
		point.Reset()

		currentInterleavedGroup++
		if currentInterleavedGroup == interleavedGenerationGroups {
			currentInterleavedGroup = 0
		}
	}
	if n != sim.SeenPoints() {
		panic(fmt.Sprintf("Logic error, written %d points, generated %d points", n, sim.SeenPoints()))
	}
	serializer.SerializeSize(out, sim.SeenPoints(), sim.SeenValues())
	err := out.Flush()
	dur := time.Now().Sub(t)
	log.Printf("Written %d points, %d values, took %0f seconds\n", n, sim.SeenValues(), dur.Seconds())
	if err != nil {
		log.Fatal(err.Error())
	}
}
