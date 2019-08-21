package main

import (
	"fmt"
	"github.com/pkg/errors"
	"time"

	"github.com/valyala/fasthttp"
)

const DefaultIdleConnectionTimeout = 90 * time.Second

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Authorization token
	Token string

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c    HTTPWriterConfig
	url  []byte
	auth string
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig) *HTTPWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "bulk_load_splunk",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},

		c:   c,
		url: []byte(c.Host + "/services/collector"),
		auth: fmt.Sprintf("Splunk %s", c.Token),
	}
}

var (
	post      = []byte("POST")
	applicationJson = []byte("application/json")
)

// WriteJsonProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteJsonProtocol(body []byte, isGzip bool) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(applicationJson)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	req.Header.Add("Authorization", w.auth)
	if isGzip {
		req.Header.Add("Content-Encoding", "gzip")
	}
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusOK {
			err = fmt.Errorf("%s - unexpected POST response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	} else {
		err = errors.Wrap(err, "POST failed")
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}
