// bulk_load_influx loads an InfluxDB daemon with data from stdin.
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
	"io"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"github.com/influxdata/influxdb-comparisons/util/report"
	"github.com/valyala/fasthttp"
)

// TODO AP: Maybe useless
const RateControlGranularity = 1000 // 1000 ms = 1s
const RateControlMinBatchSize = 100

type InfluxBulkLoad struct {
	// Program option vars:
	csvDaemonUrls     string
	daemonUrls        []string
	replicationFactor int
	ingestRateLimit   int
	backoff           time.Duration
	backoffTimeOut    time.Duration
	useGzip           bool
	consistency       string
	clientIndex       int
	organization      string // InfluxDB v2
	credentialFile    string // InfluxDB v2
	//runtime vars
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	progressIntervalItems uint64
	ingestionRateGran     float64
	maxBatchSize          int
	speedUpRequest        int32
	scanFinished          bool
	totalBackOffSecs      float64
	configs               []*workerConfig
	valuesRead            int64
	itemsRead             int64
	bytesRead             int64
	useApiV2              bool
	authToken             string // InfluxDB v2
	bucketId              string // InfluxDB v2
	orgId                 string // InfluxDB v2
}

var consistencyChoices = map[string]struct{}{
	"any":    {},
	"one":    {},
	"quorum": {},
	"all":    {},
}

type batch struct {
	Buffer *bytes.Buffer
	Items  int
	Values int
}

var load = &InfluxBulkLoad{}

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

type workerConfig struct {
	url            string
	backingOffChan chan bool
	backingOffDone chan struct{}
	writer         *HTTPWriter
	backingOffSecs float64
}

func (l *InfluxBulkLoad) Init() {
	flag.StringVar(&l.csvDaemonUrls, "urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.IntVar(&l.replicationFactor, "replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flag.StringVar(&l.consistency, "consistency", "one", "Write consistency. Must be one of: any, one, quorum, all.")
	flag.DurationVar(&l.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.DurationVar(&l.backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	flag.BoolVar(&l.useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.IntVar(&l.clientIndex, "client-index", 0, "Index of a client host running this tool. Used to distribute load")
	flag.IntVar(&l.ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in values/s (-1 = no limit).")
	flag.StringVar(&l.organization, "organization", "", "Organization name (InfluxDB v2).")
	flag.StringVar(&l.credentialFile, "credentials-file", "", "Credentials file (InfluxDB v2).")
}

func (l *InfluxBulkLoad) Validate() {
	if _, ok := consistencyChoices[l.consistency]; !ok {
		log.Fatalf("invalid consistency settings")
	}

	l.daemonUrls = strings.Split(l.csvDaemonUrls, ",")
	if len(l.daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", l.daemonUrls)

	if l.ingestRateLimit > 0 {
		l.ingestionRateGran = (float64(l.ingestRateLimit) / float64(bulk_load.Runner.Workers)) / (float64(1000) / float64(RateControlGranularity))
		log.Printf("Using worker ingestion rate %v values/%v ms", l.ingestionRateGran, RateControlGranularity)
		recommendedBatchSize := int((l.ingestionRateGran / bulk_load.ValuesPerMeasurement) * 0.20)
		log.Printf("Calculated batch size hint: %v (allowed min: %v max: %v)", recommendedBatchSize, RateControlMinBatchSize, bulk_load.Runner.BatchSize)
		if recommendedBatchSize < RateControlMinBatchSize {
			recommendedBatchSize = RateControlMinBatchSize
		} else if recommendedBatchSize > bulk_load.Runner.BatchSize {
			recommendedBatchSize = bulk_load.Runner.BatchSize
		}
		l.maxBatchSize = bulk_load.Runner.BatchSize
		if recommendedBatchSize != bulk_load.Runner.BatchSize {
			log.Printf("Adjusting batchSize from %v to %v (%v values in 1 batch)", bulk_load.Runner.BatchSize, recommendedBatchSize, float32(recommendedBatchSize)*bulk_load.ValuesPerMeasurement)
			bulk_load.Runner.BatchSize = recommendedBatchSize
		}
	} else {
		log.Print("Ingestion rate control is off")
	}

	if bulk_load.Runner.TimeLimit > 0 && l.backoffTimeOut > bulk_load.Runner.TimeLimit {
		l.backoffTimeOut = bulk_load.Runner.TimeLimit
	}

	if l.organization != "" || l.credentialFile != "" {
		if l.organization == "" {
			log.Fatalf("organization must be set for InfluxDB v2")
		}
		if l.credentialFile == "" {
			log.Fatalf("credentials-file must be set for InfluxDB v2")
		}
		l.useApiV2 = true
		log.Print("Using InfluxDB API version 2")
	}
}

func (l *InfluxBulkLoad) CreateDb() {
	listDatabasesFn := l.listDatabases
	createDbFn := l.createDb

	// use proper functions per version
	if l.credentialFile != "" {
		authTokenBytes, err := ioutil.ReadFile(l.credentialFile)
		if err != nil {
			log.Fatalf("error reading credentials file: %v", err)
		}
		l.authToken = string(authTokenBytes)
	}
	if l.organization != "" {
		organizations, err := l.listOrgs2(l.daemonUrls[0], l.organization)
		if err != nil {
			log.Fatalf("error listing organizations: %v", err)
		}
		l.orgId, _ = organizations[l.organization]
		if l.orgId == "" {
			log.Fatalf("organization '%s' not found", l.organization)
		}
		listDatabasesFn = l.listDatabases2
		createDbFn = l.createDb2
	}

	// this also test db connection
	existingDatabases, err := listDatabasesFn(l.daemonUrls[0])
	if err != nil {
		log.Fatal(err)
	}

	if len(existingDatabases) > 0 {
		if bulk_load.Runner.DoAbortOnExist {
			log.Fatalf("There are databases already in the data store. If you know what you are doing, run the command:\ncurl 'http://localhost:8086/query?q=drop%%20database%%20%s'\n", bulk_load.Runner.DbName)
		} else {
			log.Print("There are databases already in the data store.")
		}
	}

	var id string
	id, ok := existingDatabases[bulk_load.Runner.DbName]
	if ok {
		log.Printf("Database %s [%s] already exists", bulk_load.Runner.DbName, id)
	} else {
		id, err = createDbFn(l.daemonUrls[0], bulk_load.Runner.DbName)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
		log.Printf("Database %s [%s] created", bulk_load.Runner.DbName, id)
	}
	if l.useApiV2 {
		l.bucketId = id
	}
}

func (l *InfluxBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return l
}

func (l *InfluxBulkLoad) GetScanner() bulk_load.Scanner {
	return l
}

func (l *InfluxBulkLoad) PrepareWorkers() {

	l.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	l.batchChan = make(chan batch, bulk_load.Runner.Workers)
	l.inputDone = make(chan struct{})

	l.configs = make([]*workerConfig, bulk_load.Runner.Workers)
}

func (l *InfluxBulkLoad) EmptyBatchChanel() {
	for range l.batchChan {
		//read out remaining batches
	}
}

func (l *InfluxBulkLoad) SyncEnd() {
	<-l.inputDone
	close(l.batchChan)
}

func (l *InfluxBulkLoad) CleanUp() {
	for _, c := range l.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	l.totalBackOffSecs = float64(0)
	for i := 0; i < bulk_load.Runner.Workers; i++ {
		l.totalBackOffSecs += l.configs[i].backingOffSecs
	}
}

func (l *InfluxBulkLoad) PrepareProcess(i int) {
	l.configs[i] = &workerConfig{
		url:            l.daemonUrls[(i+l.clientIndex)%len(l.daemonUrls)],
		backingOffChan: make(chan bool, 100),
		backingOffDone: make(chan struct{}),
	}
	var url string
	var c *HTTPWriterConfig

	if l.useApiV2 {
		c = &HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, l.configs[i].url),
			Host:           l.configs[i].url,
			Database:       bulk_load.Runner.DbName,
			BackingOffChan: l.configs[i].backingOffChan,
			BackingOffDone: l.configs[i].backingOffDone,
			OrgId:          l.orgId,
			BucketId:       l.bucketId,
			AuthToken:      l.authToken,
		}
		url = c.Host + "/api/v2/write?org=" + c.OrgId + "&bucket=" + c.BucketId + "&precision=ns&consistency=" + l.consistency
	} else {
		c = &HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, l.configs[i].url),
			Host:           l.configs[i].url,
			Database:       bulk_load.Runner.DbName,
			BackingOffChan: l.configs[i].backingOffChan,
			BackingOffDone: l.configs[i].backingOffDone,
		}
		url = c.Host + "/write?consistency=" + l.consistency + "&db=" + neturl.QueryEscape(c.Database)
	}
	l.configs[i].writer = NewHTTPWriter(*c, url)
}

func (l *InfluxBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	return l.processBatches(l.configs[i].writer, l.configs[i].backingOffChan, telemetryPoints, fmt.Sprintf("%d", i), waitGroup, reportTags)
}
func (l *InfluxBulkLoad) AfterRunProcess(i int) {
	l.configs[i].backingOffSecs = processBackoffMessages(i, l.configs[i].backingOffChan, l.configs[i].backingOffDone)
}

func (l *InfluxBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {

	reportTags = [][2]string{{"back_off", strconv.Itoa(int(l.backoff.Seconds()))}}
	reportTags = append(reportTags, [2]string{"consistency", l.consistency})

	extraVals = make([]report.ExtraVal, 0)

	if l.ingestRateLimit > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "ingest_rate_limit_values", Value: l.ingestRateLimit})
	}
	if l.totalBackOffSecs > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "total_backoff_secs", Value: l.totalBackOffSecs})
	}

	params.DBType = "InfluxDB"
	params.DestinationUrl = l.csvDaemonUrls
	params.IsGzip = l.useGzip

	return
}

func (l *InfluxBulkLoad) IsScanFinished() bool {
	return l.scanFinished
}

func (l *InfluxBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = l.itemsRead
	bytesRead = l.bytesRead
	valuesRead = l.valuesRead
	return
}

// scan reads one item at a time from stdin. 1 item = 1 line.
// When the requested number of items per batch is met, send a batch over batchChan for the workers to write.
func (l *InfluxBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	l.scanFinished = false
	l.itemsRead = 0
	l.bytesRead = 0
	l.valuesRead = 0
	buf := l.bufPool.Get().(*bytes.Buffer)

	var n, values int
	var totalPoints, totalValues, totalValuesCounted int64

	newline := []byte("\n")
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}

	var batchItemCount uint64
	var err error
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 4*1024*1024))
outer:
	for scanner.Scan() {
		if l.itemsRead == bulk_load.Runner.ItemLimit {
			break
		}

		line := scanner.Text()
		totalPoints, totalValues, err = common.CheckTotalValues(line)
		if totalPoints > 0 || totalValues > 0 {
			continue
		} else {
			fieldCnt := countFields(line)
			values += fieldCnt
			totalValuesCounted += int64(fieldCnt)
		}
		if err != nil {
			log.Fatal(err)
		}
		l.itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= bulk_load.Runner.BatchSize {
			atomic.AddUint64(&l.progressIntervalItems, batchItemCount)
			batchItemCount = 0

			l.bytesRead += int64(buf.Len())
			l.batchChan <- batch{buf, n, values}
			buf = l.bufPool.Get().(*bytes.Buffer)
			n = 0
			values = 0

			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}

			if l.ingestRateLimit > 0 {
				if bulk_load.Runner.BatchSize < l.maxBatchSize {
					hint := atomic.LoadInt32(&l.speedUpRequest)
					if hint > int32(bulk_load.Runner.Workers*2) { // we should wait for more requests (and this is just a magic number)
						atomic.StoreInt32(&l.speedUpRequest, 0)
						bulk_load.Runner.BatchSize += int(float32(l.maxBatchSize) * 0.10)
						if bulk_load.Runner.BatchSize > l.maxBatchSize {
							bulk_load.Runner.BatchSize = l.maxBatchSize
						}
						log.Printf("Increased batch size to %d\n", bulk_load.Runner.BatchSize)
					}
				}
			}
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
	if n > 0 {
		l.batchChan <- batch{buf, n, values}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(l.inputDone)

	l.valuesRead = totalValues
	if totalValues == 0 {
		l.valuesRead = totalValuesCounted
	}
	if l.itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", l.itemsRead, totalPoints)
		}
	}
	l.scanFinished = true
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (l *InfluxBulkLoad) processBatches(w *HTTPWriter, backoffSrc chan bool, telemetrySink chan *report.Point, telemetryWorkerLabel string, workersGroup *sync.WaitGroup, reportTags [][2]string) error {
	var batchesSeen int64

	// Ingestion rate control vars
	var gvCount float64
	var gvStart time.Time

	defer workersGroup.Done()

	for batch := range l.batchChan {
		batchesSeen++

		//var bodySize int
		ts := time.Now().UnixNano()

		if l.ingestRateLimit > 0 && gvStart.Nanosecond() == 0 {
			gvStart = time.Now()
		}

		// Write the batch: try until backoff is not needed.
		if bulk_load.Runner.DoLoad {
			var err error
			sleepTime := l.backoff
			timeStart := time.Now()
			for {
				if l.useGzip {
					compressedBatch := l.bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.Buffer.Bytes())
					//bodySize = len(compressedBatch.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					// Return the compressed batch buffer to the pool.
					compressedBatch.Reset()
					l.bufPool.Put(compressedBatch)
				} else {
					//bodySize = len(batch.Bytes())
					_, err = w.WriteLineProtocol(batch.Buffer.Bytes(), false)
				}

				if err == BackoffError {
					backoffSrc <- true
					// Report telemetry, if applicable:
					if telemetrySink != nil {
						p := report.GetPointFromGlobalPool()
						p.Init("benchmarks_telemetry", ts)
						for _, tagpair := range reportTags {
							p.AddTag(tagpair[0], tagpair[1])
						}
						p.AddTag("client_type", "load")
						p.AddTag("worker", telemetryWorkerLabel)
						p.AddBoolField("backoff", true)
						telemetrySink <- p
					}
					time.Sleep(sleepTime)
					sleepTime += l.backoff        // sleep longer if backpressure comes again
					if sleepTime > 10*l.backoff { // but not longer than 10x default backoff time
						log.Printf("[worker %s] sleeping on backoff response way too long (10x %v)", telemetryWorkerLabel, l.backoff)
						sleepTime = 10 * l.backoff
					}
					checkTime := time.Now()
					if timeStart.Add(l.backoffTimeOut).Before(checkTime) {
						log.Printf("[worker %s] Spent too much time in backoff: %ds\n", telemetryWorkerLabel, int64(checkTime.Sub(timeStart).Seconds()))
						break
					}
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				return fmt.Errorf("Error writing: %s\n", err.Error())
			}
		}

		// lagMillis intentionally includes backoff time,
		// and incidentally includes compression time:
		lagMillis := float64(time.Now().UnixNano()-ts) / 1e6

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		l.bufPool.Put(batch.Buffer)

		// Normally report after each batch
		reportStat := true
		valuesWritten := float64(batch.Values)

		// Apply ingest rate control if set
		if l.ingestRateLimit > 0 {
			gvCount += valuesWritten
			if gvCount >= l.ingestionRateGran {
				now := time.Now()
				elapsed := now.Sub(gvStart)
				overDelay := (gvCount - l.ingestionRateGran) / (l.ingestionRateGran / float64(RateControlGranularity))
				remainingMs := RateControlGranularity - (elapsed.Nanoseconds() / 1e6) + int64(overDelay)
				valuesWritten = gvCount
				lagMillis = float64(elapsed.Nanoseconds() / 1e6)
				if remainingMs > 0 {
					time.Sleep(time.Duration(remainingMs) * time.Millisecond)
					gvStart = time.Now()
					realDelay := gvStart.Sub(now).Nanoseconds() / 1e6 // 'now' was before sleep
					lagMillis += float64(realDelay)
				} else {
					gvStart = now
					atomic.AddInt32(&l.speedUpRequest, 1)
				}
				gvCount = 0
			} else {
				reportStat = false
			}
		}

		// Report sent batch statistic
		if reportStat {
			stat := bulk_load.Runner.StatPool.Get().(*bulk_load.Stat)
			stat.Label = []byte(telemetryWorkerLabel)
			stat.Value = valuesWritten
			bulk_load.Runner.StatChan <- stat
		}
	}

	return nil
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) float64 {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			log.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
	return totalBackoffSecs
}

func (l *InfluxBulkLoad) createDb(daemonUrl, dbName string) (string, error) {
	u, err := neturl.Parse(daemonUrl)
	if err != nil {
		return "", err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", l.consistency)
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbName, l.replicationFactor))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("createDb returned status code: %v", resp.StatusCode)
	}
	return "", nil
}

func (l *InfluxBulkLoad) createDb2(daemonUrl, dbName string) (string, error) {
	type bucketType struct {
		ID string `json:"id,omitempty"`
		Name string `json:"name"`
		Organization string `json:"organization"`
		OrgID string `json:"orgID"`
	}
	bucket := bucketType{
		Name: dbName,
		Organization: l.organization,
		OrgID: l.orgId,
	}
	bucketBytes, err := json.Marshal(bucket)
	if err != nil {
		return "", fmt.Errorf("createDb2 marshal error: %s", err.Error())
	}

	u := fmt.Sprintf("%s/api/v2/buckets", daemonUrl)
	req, err := http.NewRequest("POST", u, bytes.NewReader(bucketBytes))
	if err != nil {
		return "", fmt.Errorf("createDb2 newRequest error: %s", err.Error())
	}
	req.Header.Add("Authorization", fmt.Sprintf("Token %s", l.authToken))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("createDb2 POST error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		return "", fmt.Errorf("createDb2 POST status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("createDb2 readAll error: %s", err.Error())
	}

	err = json.Unmarshal(body, &bucket)
	if err != nil {
		return "", fmt.Errorf("createDb2 unmarshal error: %s", err.Error())
	}

	return bucket.ID, nil
}

// listDatabases lists the existing databases in InfluxDB.
func (l *InfluxBulkLoad) listDatabases(daemonUrl string) (map[string]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases get error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listDatabases returned status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listDatabases readAll error: %s", err.Error())
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listDatabases unmarshal error: %s", err.Error())
	}

	ret := make(map[string]string)
	for _, nestedName := range listing.Results[0].Series[0].Values {
		ret[nestedName[0]] = ""
	}
	return ret, nil
}

// listDatabases2 lists the existing databases in InfluxDB v2
func (l *InfluxBulkLoad) listDatabases2(daemonUrl string) (map[string]string, error) {
	u := fmt.Sprintf("%s/api/v2/buckets", daemonUrl)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("listDatabases2 newRequest error: %s", err.Error())
	}
	req.Header.Add("Authorization", fmt.Sprintf("Token %s", l.authToken))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listDatabases2 GET error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listDatabases2 GET status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listDatabases2 readAll error: %s", err.Error())
	}

	// Do ad-hoc parsing to find existing database names:
	// {"buckets":[{"name:test_db","id":"1","organization":"test_org","organizationID":"2"},...]}%
	type listingType struct {
		Buckets []struct {
			Id string
			Organization string
			OrgID string
			Name string
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listDatabases2 unmarshal error: %s", err.Error())
	}

	ret := make(map[string]string)
	for _, bucket := range listing.Buckets {
		ret[bucket.Name] = bucket.Id
	}
	return ret, nil
}

// listOrgs2 lists the organizations in InfluxDB v2
func (l *InfluxBulkLoad) listOrgs2(daemonUrl string, orgName string) (map[string]string, error) {
	u := fmt.Sprintf("%s/api/v2/orgs", daemonUrl)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 newRequest error: %s", err.Error())
	}
	req.Header.Add("Authorization", fmt.Sprintf("Token %s", l.authToken))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 GET error: %s", err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listOrgs2 GET status code: %v", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("listOrgs2 readAll error: %s", err.Error())
	}

	type listingType struct {
		Orgs []struct {
			Id string
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

// countFields return number of fields in protocol line
func countFields(line string) int {
	lineParts := strings.Split(line, " ") // "measurement,tags fields timestamp"
	if len(lineParts) != 3 {
		log.Fatalf("invalid protocol line: '%s'", line)
	}
	fieldCnt := strings.Count(lineParts[1], "=")
	if fieldCnt == 0 {
		log.Fatalf("invalid fields parts: '%s'", lineParts[1])
	}
	return fieldCnt
}
