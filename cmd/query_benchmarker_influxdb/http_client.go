package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"
	"net"
	"github.com/valyala/fasthttp"
)

var bytesSlash = []byte("/") // heap optimization
var byteZero = []byte{0}     // heap optimization

// HTTPClient is a reusable HTTP Client.
type HTTPClient struct {
	client     fasthttp.Client
	Host       []byte
	HostString string
	debug      int
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
}

// NewHTTPClient creates a new HTTPClient.
func NewHTTPClient(host string, debug int, timeout time.Duration) *HTTPClient {
	return &HTTPClient{
		client: fasthttp.Client{
			Name: "query_benchmarker",
			Dial: func(addr string) (net.Conn, error) {
				return fasthttp.DialTimeout(addr, timeout)
			},
		},
		Host:       []byte(host),
		HostString: host,
		debug:      debug,
	}
}

// Do performs the action specified by the given Query. It uses fasthttp, and
// tries to minimize heap allocations.
func (w *HTTPClient) Do(q *Query, opts *HTTPClientDoOptions) (lag float64, err error) {
	// populate uri from the reusable byte slice:
	uri := make([]byte, 0, 100)
	uri = append(uri, w.Host...)
	uri = append(uri, bytesSlash...)
	uri = append(uri, q.Path...)

	// populate a request with data from the Query:
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethodBytes(q.Method)
	req.Header.SetRequestURIBytes(uri)
	req.SetBody(q.Body)
	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	start := time.Now()
	err = w.client.Do(req, resp)
	lag = float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	if err != nil || resp.StatusCode() != fasthttp.StatusOK {
		values, _ := url.ParseQuery(string(uri))
		fmt.Printf("debug: url: %s, path %s, parsed url - %s\n", string(uri), q.Path, values)
	}

	// Check that the status code was 200 OK:
	if err == nil {
		sc := resp.StatusCode()
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

				_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, pretty)
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
