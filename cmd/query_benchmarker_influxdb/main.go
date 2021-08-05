// query_benchmarker speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	nethttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_query"
	"github.com/influxdata/influxdb-comparisons/bulk_query/http"
	"github.com/influxdata/influxdb-comparisons/util/report"
)

// Program option vars:
type InfluxQueryBenchmarker struct {
	csvDaemonUrls string
	daemonUrls    []string
	organization  string // InfluxDB v2
	token         string // InfluxDB v2

	dialTimeout    time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	httpClientType string
	clientIndex    int
	scanFinished   bool

	queryPool sync.Pool
	queryChan chan []*http.Query

	useApiV2            bool
	useCompatibilityApi bool
	bucketId            string // InfluxDB v2
	orgId               string // InfluxDB v2
}

var querier = &InfluxQueryBenchmarker{}

// Parse args:
func init() {

	bulk_query.Benchmarker.Init()
	querier.Init()

	flag.Parse()

	bulk_query.Benchmarker.Validate()
	querier.Validate()

}

func (b *InfluxQueryBenchmarker) Init() {
	flag.StringVar(&b.csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.DurationVar(&b.dialTimeout, "dial-timeout", time.Second*15, "TCP dial timeout.")
	flag.DurationVar(&b.readTimeout, "write-timeout", time.Second*300, "TCP write timeout.")
	flag.DurationVar(&b.writeTimeout, "read-timeout", time.Second*300, "TCP read timeout.")
	flag.StringVar(&b.httpClientType, "http-client-type", "fast", "HTTP client type {fast, default}")
	flag.IntVar(&b.clientIndex, "client-index", 0, "Index of a client host running this tool. Used to distribute load")
	flag.StringVar(&b.organization, "organization", "", "Organization name (InfluxDB v2).")
	flag.StringVar(&b.token, "token", "", "Authentication token (InfluxDB v2).")
	flag.BoolVar(&b.useCompatibilityApi, "use-compatibility", false, "Use compatibility /query API - for running InfluxQL with InfluxDB 2.x")
}

func (b *InfluxQueryBenchmarker) Validate() {
	b.daemonUrls = strings.Split(b.csvDaemonUrls, ",")
	if len(b.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	log.Printf("daemon URLs: %v\n", b.daemonUrls)

	if b.httpClientType == "fast" || b.httpClientType == "default" {
		log.Printf("Using HTTP client: %v\n", b.httpClientType)
		http.UseFastHttp = b.httpClientType == "fast"
	} else {
		log.Fatalf("Unsupported HTPP client type: %v", b.httpClientType)
	}

	if !b.useCompatibilityApi && (b.organization != "" || b.token != "") {
		if b.organization == "" {
			log.Fatal("organization must be specified for InfluxDB 2.x")
		}
		if b.token == "" {
			log.Fatal("token must be specified for InfluxDB 2.x")
		}
		organizations, err := b.listOrgs2(b.daemonUrls[0], b.organization)
		if err != nil {
			log.Fatalf("error listing organizations: %v", err)
		}
		b.orgId, _ = organizations[b.organization]
		if b.orgId == "" {
			log.Fatalf("organization '%s' not found", b.organization)
		}
		b.useApiV2 = true
		log.Print("Using InfluxDB API version 2")
	}

	if b.useCompatibilityApi && b.token == "" {
		log.Fatal("token must be provided when using compatibility API")
	}
}

func (b *InfluxQueryBenchmarker) Prepare() {
	// Make pools to minimize heap usage:
	b.queryPool = sync.Pool{
		New: func() interface{} {
			return &http.Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				Method:           make([]byte, 0, 1024),
				Path:             make([]byte, 0, 1024),
				Body:             make([]byte, 0, 1024),
			}
		},
	}

	// Make data and control channels:
	b.queryChan = make(chan []*http.Query)
}

func (b *InfluxQueryBenchmarker) GetProcessor() bulk_query.Processor {
	return b
}
func (b *InfluxQueryBenchmarker) GetScanner() bulk_query.Scanner {
	return b
}

func (b *InfluxQueryBenchmarker) PrepareProcess(i int) {
}

func (b *InfluxQueryBenchmarker) RunProcess(i int, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) {
	daemonUrl := b.daemonUrls[(i+b.clientIndex)%len(b.daemonUrls)]
	w := http.NewHTTPClient(daemonUrl, bulk_query.Benchmarker.Debug(), b.dialTimeout, b.readTimeout, b.writeTimeout)
	b.processQueries(w, workersGroup, statPool, statChan)
}

func (b *InfluxQueryBenchmarker) IsScanFinished() bool {
	return b.scanFinished
}

func (b *InfluxQueryBenchmarker) CleanUp() {
	close(b.queryChan)
}

func (b InfluxQueryBenchmarker) UpdateReport(params *report.QueryReportParams, reportTags [][2]string, extraVals []report.ExtraVal) (updatedTags [][2]string, updatedExtraVals []report.ExtraVal) {
	params.DBType = "InfluxDB"
	params.DestinationUrl = b.csvDaemonUrls
	updatedTags = reportTags
	updatedExtraVals = extraVals
	return
}

func main() {
	bulk_query.Benchmarker.RunBenchmark(querier)
}

var qind int64

// scan reads encoded Queries and places them onto the workqueue.
func (b *InfluxQueryBenchmarker) RunScan(r io.Reader, closeChan chan int) {
	dec := gob.NewDecoder(r)

	batch := make([]*http.Query, 0, bulk_query.Benchmarker.BatchSize())

	i := 0
loop:
	for {
		if bulk_query.Benchmarker.Limit() >= 0 && qind >= bulk_query.Benchmarker.Limit() {
			break
		}

		q := b.queryPool.Get().(*http.Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = qind
		batch = append(batch, q)
		i++
		if i == bulk_query.Benchmarker.BatchSize() {
			b.queryChan <- batch
			//batch = batch[:0]
			batch = nil
			batch = make([]*http.Query, 0, bulk_query.Benchmarker.BatchSize())
			i = 0
		}

		qind++
		select {
		case <-closeChan:
			log.Println("Received finish request")
			break loop
		default:
		}

	}
	b.scanFinished = true
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func (b *InfluxQueryBenchmarker) processQueries(w http.HTTPClient, workersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *bulk_query.Stat) error {
	opts := &http.HTTPClientDoOptions{
		Debug:                bulk_query.Benchmarker.Debug(),
		PrettyPrintResponses: bulk_query.Benchmarker.PrettyPrintResponses(),
	}
	if b.useApiV2 {
		opts.ContentType = "application/vnd.flux"
		opts.Accept = "application/csv"
		opts.AuthToken = b.token
		opts.Path = []byte(fmt.Sprintf("/api/v2/query?orgID=%s", b.orgId)) // query path is empty for 2.x in generated queries
	}
	// enable InfluxQL queries with 2.x
	if b.useCompatibilityApi {
		opts.AuthToken = b.token
	}

	var queriesSeen int64
	for queries := range b.queryChan {
		// enable flux queries with 1.x
		if !b.useApiV2 && strings.Contains(fmt.Sprintf("%s", queries[0].HumanLabel), "Flux") {
			opts.ContentType = "application/vnd.flux"
			opts.Accept = "application/csv"
			opts.Path = []byte("/api/v2/query")
		}
		if len(queries) == 1 {
			if err := b.processSingleQuery(w, queries[0], opts, nil, nil, statPool, statChan); err != nil {
				log.Fatal(err)
			}
			queriesSeen++
		} else {
			var err error
			errors := 0
			done := 0
			errCh := make(chan error)
			doneCh := make(chan int, len(queries))
			for _, q := range queries {
				go b.processSingleQuery(w, q, opts, errCh, doneCh, statPool, statChan)
				queriesSeen++
				if bulk_query.Benchmarker.GradualWorkersIncrease() {
					time.Sleep(time.Duration(rand.Int63n(150)) * time.Millisecond) // random sleep 0-150ms
				}
			}

		loop:
			for {
				select {
				case err = <-errCh:
					errors++
				case <-doneCh:
					done++
					if done == len(queries) {
						break loop
					}
				}
			}
			close(errCh)
			close(doneCh)
			if err != nil {
				log.Fatal(err)
			}
		}
		if bulk_query.Benchmarker.WaitInterval().Seconds() > 0 {
			time.Sleep(bulk_query.Benchmarker.WaitInterval())
		}
	}
	workersGroup.Done()
	return nil
}

func (b *InfluxQueryBenchmarker) processSingleQuery(w http.HTTPClient, q *http.Query, opts *http.HTTPClientDoOptions, errCh chan error, doneCh chan int, statPool sync.Pool, statChan chan *bulk_query.Stat) error {
	defer func() {
		if doneCh != nil {
			doneCh <- 1
		}
	}()
	if b.useApiV2 || strings.Contains(fmt.Sprintf("%s", q.HumanLabel), "Flux") {
		q.Path = opts.Path
	}
	lagMillis, err := w.Do(q, opts)
	stat := statPool.Get().(*bulk_query.Stat)
	stat.Init(q.HumanLabel, lagMillis)
	statChan <- stat
	b.queryPool.Put(q)
	if err != nil {
		qerr := fmt.Errorf("Error during request of query %s: %s\n", q.String(), err.Error())
		if errCh != nil {
			errCh <- qerr
			return nil
		} else {
			return qerr
		}
	}

	return nil
}

func (l *InfluxQueryBenchmarker) listOrgs2(daemonUrl string, orgName string) (map[string]string, error) {
	u := fmt.Sprintf("%s/api/v2/orgs", daemonUrl)
	req, err := nethttp.NewRequest(nethttp.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 newRequest error: %s", err.Error())
	}
	req.Header.Add("Authorization", fmt.Sprintf("Token %s", l.token))

	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 GET error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != nethttp.StatusOK {
		return nil, fmt.Errorf("listOrgs2 GET status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 readAll error: %s", err.Error())
	}

	type listingType struct {
		Orgs []struct {
			Id   string
			Name string
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listOrgs unmarshal error: %s", err.Error())
	}

	ret := make(map[string]string)
	for _, org := range listing.Orgs {
		ret[org.Name] = org.Id
	}
	return ret, nil
}
