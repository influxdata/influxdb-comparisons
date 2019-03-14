// bulk_load_es loads an ElasticSearch daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/valyala/fasthttp"
	"strconv"
)

// Args parsing vars
var (
	indexTemplateChoices = map[string]map[string][]byte{
		"default": {
			"5": defaultTemplate,
			"6": defaultTemplate6x,
		},
		"aggregation": {
			"5": aggregationTemplate,
			"6": aggregationTemplate6x,
		},
	}
)

var defaultTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "point": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
        "timestamp":    { "type": "date", "doc_values": true }
      }
    }
  }
}
`)

var aggregationTemplate = []byte(`
{
  "template": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}}
    }
  },
  "mappings": {
    "_default_": {
      "dynamic_templates": [
        {
          "all_string_fields_can_be_used_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "string",
              "doc_values": true,
              "index": "not_analyzed"
            }
          }
        },
        {
          "all_nonstring_fields_are_just_stored_in_column_index": {
            "match": "*",
            "match_mapping_type": "*",
            "mapping": {
              "doc_values": true,
              "index": "no"
            }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": {
          "type": "date",
          "doc_values": true,
          "index": "not_analyzed"
        }
      }
    }
  }
}

`)

var defaultTemplate6x = []byte(`
{
  "index_patterns": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}},
      "translog.retention.size": "20mb",
      "translog.retention.age" : "10s",
      "translog.flush_threshold_size" : "20mb"
    }
  },
  "mappings": {
    "_doc": {
      "_all":            { "enabled": false },
      "_source":         { "enabled": true },
      "properties": {
        "timestamp":    { "type": "date", "doc_values": true }
      }
    }
  }
}
`)

var aggregationTemplate6x = []byte(`
{
  "index_patterns": "*",
  "settings": {
    "index": {
      "refresh_interval": "5s",
      "number_of_replicas": {{.NumberOfReplicas}},
      "number_of_shards": {{.NumberOfShards}},
      "translog.retention.size": "20mb",
      "translog.retention.age" : "10s",
      "translog.flush_threshold_size" : "20mb"
    }
  },
  "mappings": {
    "_doc": {
      "dynamic_templates": [
        {
          "all_string_fields_can_be_used_for_filtering": {
            "match": "*",
            "match_mapping_type": "string",
            "mapping": {
              "type": "keyword",
              "doc_values": true
            }
          }
        },
        {
          "all_nonstring_fields_are_just_stored_in_column_index": {
            "match": "*",
            "match_mapping_type": "*",
            "mapping": {
              "doc_values": true,
              "index": false
            }
          }
        }
      ],
      "_all": { "enabled": false },
      "_source": { "enabled": false },
      "properties": {
        "timestamp": {
          "type": "date",
          "doc_values": true,
          "index": true
        }
      }
    }
  }
}
`)

type ElasticBulkLoad struct {
	// Program option vars:
	csvDaemonUrls     string
	daemonUrls        []string
	refreshEachBatch  bool
	indexTemplateName string
	useGzip           bool
	numberOfReplicas  uint
	numberOfShards    uint
	// Global vars
	bufPool      sync.Pool
	batchChan    chan *bytes.Buffer
	inputDone    chan struct{}
	scanFinished bool
}

var load = &ElasticBulkLoad{}

// Parse args:
func init() {
	bulk_load.Runner.Init(5000)
	load.Init()

	flag.Parse()

	bulk_load.Runner.Validate()
	load.Validate()

}

func main() {
	bulk_load.Runner.Run(load)
}

func (l *ElasticBulkLoad) Init() {
	flag.StringVar(&l.csvDaemonUrls, "urls", "http://localhost:9200", "ElasticSearch URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.BoolVar(&l.refreshEachBatch, "refresh", true, "Whether each batch is immediately indexed.")

	flag.StringVar(&l.indexTemplateName, "index-template", "default", "ElasticSearch index template to use (choices: default, aggregation).")

	flag.BoolVar(&l.useGzip, "gzip", true, "Whether to gzip encode requests (default true).")

	flag.UintVar(&l.numberOfReplicas, "number-of-replicas", 0, "Number of ES replicas (note: replicas == replication_factor - 1). Zero replicas means RF of 1.")
	flag.UintVar(&l.numberOfShards, "number-of-shards", 1, "Number of ES shards. Typically you will set this to the number of nodes in the cluster.")

}

func (l *ElasticBulkLoad) Validate() {
	l.daemonUrls = strings.Split(l.csvDaemonUrls, ",")
	if len(l.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", l.daemonUrls)

	if _, ok := indexTemplateChoices[l.indexTemplateName]; !ok {
		log.Fatalf("invalid index template type")
	}
}

func (l *ElasticBulkLoad) CreateDb() {
	v, err := checkServer(l.daemonUrls[0])
	if err != nil {
		log.Fatal(err)
	}
	if bulk_load.Runner.DoDBCreate {
		// check that there are no pre-existing index templates:
		existingIndexTemplates, err := listIndexTemplates(l.daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndexTemplates) > 0 {
			log.Println("There are index templates already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_template/*'")
		}

		// check that there are no pre-existing indices:
		existingIndices, err := listIndices(l.daemonUrls[0])
		if err != nil {
			log.Fatal(err)
		}

		if len(existingIndices) > 0 {
			log.Println("There are indices already in the data store. If you know what you are doing, clear them first with a command like:\ncurl -XDELETE 'http://localhost:9200/_all'")
		}

		// create the index template:
		indexTemplate := indexTemplateChoices[l.indexTemplateName]
		err = createESTemplate(l.daemonUrls[0], "measurements_template", indexTemplate[v], l.numberOfReplicas, l.numberOfShards)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (l *ElasticBulkLoad) PrepareWorkers() {
	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan *bytes.Buffer, bulk_load.Runner.Workers)
	l.inputDone = make(chan struct{})
}

func (l *ElasticBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *ElasticBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *ElasticBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *ElasticBulkLoad) CleanUp() {

}

func (l *ElasticBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {

	reportTags = [][2]string{{"replicas", strconv.Itoa(int(l.numberOfReplicas))}}
	reportTags = append(reportTags, [2]string{"shards", strconv.Itoa(int(l.numberOfShards))})
	reportTags = append(reportTags, [2]string{"index-template", l.indexTemplateName})

	params.DBType = "ElasticSearch"
	params.DestinationUrl = l.csvDaemonUrls
	params.IsGzip = l.useGzip

	return
}

func (l *ElasticBulkLoad) PrepareProcess(i int) {

}

func (l *ElasticBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	daemonUrl := l.daemonUrls[i%len(l.daemonUrls)]

	cfg := HTTPWriterConfig{
		Host: daemonUrl,
	}
	return l.processBatches(NewHTTPWriter(cfg, l.refreshEachBatch), waitGroup, telemetryPoints, fmt.Sprintf("%d", i))
}

func (l *ElasticBulkLoad) AfterRunProcess(i int) {

}

func (l *ElasticBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *ElasticBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

// scan reads items from stdin. It expects input in the ElasticSearch bulk
// format: two line pairs, the first line being an 'action' and the second line
// being the payload. (2 lines = 1 item)
func (l *ElasticBulkLoad) RunScanner(syncChanDone chan int) (int64, int64, int64) {
	l.scanFinished = false
	buf := l.bufPool.Get().(*bytes.Buffer)

	var linesRead int64
	var err error
	var itemsRead, bytesRead int64
	var totalPoints, totalValues int64

	var itemsThisBatch int
	scanner := bufio.NewScanner(os.Stdin)

	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}
outer:
	for scanner.Scan() {

		totalPoints, totalValues, err = common.CheckTotalValues(scanner.Text())
		if totalPoints > 0 || totalValues > 0 {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}

		linesRead++

		buf.Write(scanner.Bytes())
		buf.Write([]byte("\n"))

		//n++
		if linesRead%2 == 0 {
			itemsRead++
			itemsThisBatch++
		}

		hitLimit := bulk_load.Runner.ItemLimit >= 0 && itemsRead >= bulk_load.Runner.ItemLimit

		if itemsThisBatch == bulk_load.Runner.BatchSize || hitLimit {
			bytesRead += int64(buf.Len())
			l.batchChan <- buf
			buf = l.bufPool.Get().(*bytes.Buffer)
			itemsThisBatch = 0
			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}
		}

		if hitLimit {
			break
		}
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if itemsThisBatch > 0 {
		l.batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	// The ES bulk format uses 2 lines per item:
	if linesRead%2 != 0 {
		log.Fatalf("the number of lines read was not a multiple of 2, which indicates a bad bulk format for Elastic")
	}
	if itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read items: %d, expected: %d:", itemsRead, totalPoints)
		} else {
			totalValues = int64(float64(itemsRead) * bulk_load.ValuesPerMeasurement) // needed for statistics summary
		}
	}

	l.scanFinished = true
	return itemsRead, bytesRead, totalValues
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *ElasticBulkLoad) processBatches(w *HTTPWriter, workersGroup *sync.WaitGroup, telemetrySink chan *report.Point, telemetryWorkerLabel string) error {
	var batchesSeen int64
	var rerr error
	for batch := range l.batchChan {
		batchesSeen++
		if !bulk_load.Runner.DoLoad {
			continue
		}

		var err error
		var bodySize int

		// Write the batch.
		if l.useGzip {
			compressedBatch := l.bufPool.Get().(*bytes.Buffer)
			fasthttp.WriteGzip(compressedBatch, batch.Bytes())
			bodySize = len(compressedBatch.Bytes())
			_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
			// Return the compressed batch buffer to the pool.
			compressedBatch.Reset()
			l.bufPool.Put(compressedBatch)
		} else {
			bodySize = len(batch.Bytes())
			_, err = w.WriteLineProtocol(batch.Bytes(), false)
		}

		if err != nil {
			rerr = fmt.Errorf("Error writing: %s\n", err.Error())
			break
		}

		// Return the batch buffer to the pool.
		batch.Reset()
		l.bufPool.Put(batch)

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := report.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			p.AddTag("src_addr", telemetryWorkerLabel)
			p.AddTag("dst_addr", w.c.Host)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddBoolField("gzip", l.useGzip)
			p.AddInt64Field("body_bytes", int64(bodySize))
			telemetrySink <- p
		}
	}
	workersGroup.Done()
	return rerr
}

// createESTemplate uses a Go text/template to create an ElasticSearch index
// template. (This terminological conflict is mostly unavoidable).
func createESTemplate(daemonUrl, indexTemplateName string, indexTemplateBodyTemplate []byte, numberOfReplicas, numberOfShards uint) error {
	// set up URL:
	u, err := url.Parse(daemonUrl)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("_template/%s", indexTemplateName)

	// parse and execute the text/template:
	t := template.Must(template.New("index_template").Parse(string(indexTemplateBodyTemplate)))
	var body bytes.Buffer
	params := struct {
		NumberOfReplicas uint
		NumberOfShards   uint
	}{
		NumberOfReplicas: numberOfReplicas,
		NumberOfShards:   numberOfShards,
	}
	err = t.Execute(&body, params)
	if err != nil {
		return err
	}

	// do the HTTP PUT request with the body data:
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("bad mapping create: %s", body)
	}
	return nil
}

// listIndexTemplates lists the existing index templates in ElasticSearch.
func listIndexTemplates(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/_template", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}

// listIndices lists the existing indices in ElasticSearch.
func listIndices(daemonUrl string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/*", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	return listing, nil
}

// checkServer pings  ElasticSearch and returns major version string
func checkServer(daemonUrl string) (string, error) {
	majorVer := "5"
	resp, err := http.Get(daemonUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var listing map[string]interface{}
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return "", err
	}
	if v, ok := listing["version"]; ok {
		vo := v.(map[string]interface{})
		if ver, ok := vo["number"]; ok {
			fmt.Printf("Elastic Search version %s\n", ver)
			nums := strings.Split(ver.(string), ".")
			if len(nums) > 0 {
				majorVer = nums[0]
			}
		}
	}

	return majorVer, nil
}
