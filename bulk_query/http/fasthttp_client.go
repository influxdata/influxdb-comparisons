package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/influxdata/influxdb-comparisons/util/statemanager"
)

var bytesSlash = []byte("/") // heap optimization
var byteZero = []byte{0}     // heap optimization

// HTTPClient is a reusable HTTP Client.
type FastHTTPClient struct {
	HTTPClientCommon
	client fasthttp.Client
}

// NewHTTPClient creates a new HTTPClient.
func NewFastHTTPClient(host string, debug int, dialTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) *FastHTTPClient {
	return &FastHTTPClient{
		client: fasthttp.Client{
			Name: "bulk_query",
			Dial: func(addr string) (net.Conn, error) {
				return fasthttp.DialTimeout(addr, dialTimeout)
			},
			MaxIdleConnDuration: idleConnectionTimeout,
			ReadTimeout:         readTimeout,
			WriteTimeout:        writeTimeout,
		},
		HTTPClientCommon: HTTPClientCommon{
			Host:       []byte(host),
			HostString: host,
			debug:      debug,
		},
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *FastHTTPClient) Do(q *Query, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	uri := make([]byte, 0, 100)
	uri = append(uri, w.Host...)
	uri = append(uri, bytesSlash...)
	uri = append(uri, q.Path...)
	//fmt.Println("********** Query String = ", q.String())

	// Get InfluxDB V2 Init Params
	sm := statemanager.GetManager()
	v2Host := sm.Getv2Host()
	orgId := sm.GetOrgId()
	bucketId := sm.GetBucketId()
	authToken := sm.GetAuthToken()

	fmt.Println("***********************", v2Host, orgId, bucketId, authToken)

	uriV2 := make([]byte, 0, 100)
	uriV2 = append(uriV2, v2Host...)
	uriV2 = append(uriV2, bytesSlash...)
	urlV2Path := fmt.Sprintf("%s/api/v2/query?org=%s", strings.TrimSuffix(v2Host, "/"), url.QueryEscape(orgId))
	uV2Path := []byte(urlV2Path) // convert string into bytes
	fmt.Println("urlV2 =", urlV2Path)
	uriV2 = append(uriV2, urlV2Path...)
	/*
			url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(v2Host, "/"), url.QueryEscape(orgId), url.QueryEscape(bucketId))
		    u := []byte(url) // convert string into bytes
		    fmt.Println("url =", url)
		    req.Header.SetRequestURIBytes(u)
		    //req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", l.authToken))
		    req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", authToken))
	*/
	/*
			       sbRequestBody := chilkat.NewStringBuilder()
			   	   sbRequestBody.Append("from(bucket:\"example-bucket\")\n")
			       sbRequestBody.Append("        |> range(start:-1000h)\n")
			       sbRequestBody.Append("        |> group(columns:[\"_measurement\"], mode:\"by\")\n")
			       sbRequestBody.Append("        |> sum()")
		​
			       rest.AddHeader("Content-type","application/vnd.flux")
			       rest.AddHeader("Accept","application/csv")
			       rest.AddHeader("Authorization","Token YOURAUTHTOKEN")
		​
			       sbResponseBody := chilkat.NewStringBuilder()
			       success = rest.FullRequestSb("POST","/api/v2/query?org=my-org",sbRequestBody,sbResponseBody)
	*/
	// populate a request with data from the Query:
	req := fasthttp.AcquireRequest()
	req2 := fasthttp.AcquireRequest() // InfluxDB V2
	if v2Host == "" {
		defer fasthttp.ReleaseRequest(req)
		req.Header.SetMethodBytes(q.Method)
		req.Header.SetRequestURIBytes(uri)
		if opts.Authorization != "" {
			req.Header.Add("Authorization", opts.Authorization)
		}
		req.SetBody(q.Body)
	} else { // InfluxDB V2
		// populate a request with data from the Query:
		defer fasthttp.ReleaseRequest(req2)
		req2.Header.SetMethodBytes([]byte("POST"))
		req2.Header.SetRequestURIBytes(uV2Path)
		bodyV2 := make([]byte, 0, 100)
		bucketStr := fmt.Sprintf("from(bucket:\"%s\")\n", bucketId)
		//bodyV2 = append(bodyV2, "from(bucket:\"perf-reference-test-v2-bucket000\")\n"...)

		//from(bucket: "ame=perf-reference-test-v2-bucket000") |> range(start: -7d) |> yield()
		/*
			bodyV2 = append(bodyV2, bucketStr...)
			bodyV2 = append(bodyV2, "        |> range(start: -7d)\n"...)
			bodyV2 = append(bodyV2, "        |> yield()"...)
			req2.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", authToken))
			req2.Header.Set("Content-type", "application/vnd.flux")
			req2.Header.Set("Accept", "application/csv")
			req2.SetBody(bodyV2)
		*/

		bodyV2 = append(bodyV2, bucketStr...)
		bodyV2 = append(bodyV2, "        |> range(start: -1s)\n"...)
		//bodyV2 = append(bodyV2, "        |> range(start: 2019-05-13T15:17:29.000000000Z, stop: 2019-05-20T15:17:29.000000000Z)\n"...)
		//bodyV2 = append(bodyV2, "        |> filter(fn:(r) => r._measurement == \"cpu\" and r._field == \"usage_user\")\n"...)
		//bodyV2 = append(bodyV2, "        |> group()"...)
		//bodyV2 = append(bodyV2, "        |> mean()"...)
		//bodyV2 = append(bodyV2, "        |> yield()"...)
		req2.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", authToken))
		req2.Header.Set("Content-type", "application/vnd.flux")
		req2.Header.Set("Accept", "application/csv")
		req2.SetBody(bodyV2)

	}
	/*
		from(bucket: "perf-reference-test-v2-bucket000")
			|> range(start: 2019-05-13T15:17:29.000000000Z, stop: 2019-05-20T15:17:29.000000000Z)
			|> filter(fn:(r) => r._measurement == "cpu" and r._field == "usage_user")
			|> group()
			|> mean()
			|> yield()
	*/

	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	start := time.Now()
	if v2Host == "" {
		time.Sleep(12 * 1750000) // MM
		err = w.client.Do(req, resp)
	} else { // Influx DB V2
		err = w.client.Do(req2, resp)
	}
	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	if v2Host == "" {
		time.Sleep(5 * (1000000 - 10000)) // MM
	} else {
	}
	if (err != nil || resp.StatusCode() != fasthttp.StatusOK) && opts.Debug == 5 {
		values, _ := url.ParseQuery(string(uri))
		fmt.Printf("debug: url: %s, path %s, parsed url - %s\n", string(uri), q.Path, values)
	}
	// Check that the status code was 200 OK:
	if err == nil {
		sc := resp.StatusCode()
		fmt.Println("bulk_query status code = ", sc) // MM
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
			return
		}
	}
	if opts != nil {
		// Print debug messages, if applicable:
		switch opts.Debug {
		case 1:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
		case 2:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		case 3:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
		case 4:
			fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
			fmt.Fprintf(os.Stderr, "debug:   request: %s\n", string(q.String()))
			fmt.Fprintf(os.Stderr, "debug:   response: %s\n", string(resp.Body()))
		default:
		}
		// Pretty print JSON responses, if applicable:
		if opts.PrettyPrintResponses {
			// InfluxQL responses are in JSON and can be pretty-printed here.
			// Flux responses are just simple CSV.
			prefix := fmt.Sprintf("ID %d: ", q.ID)
			if json.Valid(resp.Body()) {
				var pretty bytes.Buffer
				err = json.Indent(&pretty, resp.Body(), prefix, "  ")
				if err != nil {
					return
				}
				_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, pretty.String())
				if err != nil {
					return
				}
			} else {
				_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, resp.Body())
				if err != nil {
					return
				}
			}
		}
	}
	return lag, err
}

func (w *FastHTTPClient) HostString() string {
	return w.HTTPClientCommon.HostString
}
