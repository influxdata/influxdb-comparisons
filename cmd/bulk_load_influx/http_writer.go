package main

// This file lifted wholesale from mountainflux by Mark Rushakoff.

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
)

const DefaultIdleConnectionTimeout = 90 * time.Second

var (
	BackoffError        error  = fmt.Errorf("backpressure is needed")
	backoffMagicWords0  []byte = []byte("engine: cache maximum memory size exceeded")
	backoffMagicWords1  []byte = []byte("write failed: hinted handoff queue not empty")
	backoffMagicWords2a []byte = []byte("write failed: read message type: read tcp")
	backoffMagicWords2b []byte = []byte("i/o timeout")
	backoffMagicWords3  []byte = []byte("write failed: engine: cache-max-memory-size exceeded")
	backoffMagicWords4  []byte = []byte("timeout")
	backoffMagicWords5  []byte = []byte("write failed: can not exceed max connections of 500")
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string

	BackingOffChan chan bool
	BackingOffDone chan struct{}

	// Debug label for more informative errors.
	DebugInfo string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig, consistency string) *HTTPWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name:                "bulk_load_influx",
			MaxIdleConnDuration: DefaultIdleConnectionTimeout,
		},

		c:   c,
		url: []byte(c.Host + "/write?consistency=" + consistency + "&db=" + url.QueryEscape(c.Database)),
	}
}

var (
	post      = []byte("POST")
	textPlain = []byte("text/plain")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte, isGzip bool) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
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
		if sc == 500 && backpressurePred(resp.Body()) {
			err = BackoffError
			log.Printf("backoff suggested, reason: %s", resp.Body())
		} else if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}

// WriteLineProtocolV2 (InfluxDB V2) writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocolV2(body []byte, isGzip bool, host string, orgId string, bucketId string, authToken string) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	//url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(l.v2Host, "/"), url.QueryEscape(l.orgId), url.QueryEscape(l.bucketId))
	//fmt.Println("***** ====>", host, orgId, bucketId, authToken)
	//myhost := "https://eu-central-1-1.aws.cloud2.influxdata.com/"
	//myorgId := "perf-v2"

	//mybucketId := "perf-v2-bucket"
	//myauthToken := "LN1hZhkVK7FNfK2-fJENOKCMIUlpggfwlbbG60PL2-Ot0dgDvMEcqDhPtSZqgNoW9Zqp86kpk3BsplFqXfbvOA==" // good auth token EU all access

	//url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(myhost, "/"), url.QueryEscape(myorgId), url.QueryEscape(mybucketId))
	url := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(host, "/"), url.QueryEscape(orgId), url.QueryEscape(bucketId))
	u := []byte(url) // convert string into bytes
	//fmt.Println("url =", url)
	req.Header.SetRequestURIBytes(u)
	//req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", l.authToken))
	//req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", myauthToken))
	req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", authToken))

	if isGzip {
		req.Header.Add("Content-Encoding", "gzip")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		//fmt.Println("status code = ", sc)
		if sc == 500 && backpressurePred(resp.Body()) {
			err = BackoffError
			log.Printf("backoff suggested, reason: %s", resp.Body())
		} else if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("[DebugInfo: %s] Invalid write response (status %d): %s", w.c.DebugInfo, sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
	/*
		//	u := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(sim.Host, "/"), url.QueryEscape(org), url.QueryEscape(bucket))
		u := fmt.Sprintf("%s/api/v2/write?org=%s&bucket=%s", strings.TrimSuffix(l.v2Host, "/"), url.QueryEscape(l.orgId), url.QueryEscape(l.bucketId))
		req, err := http.NewRequest("POST", u, bytes.NewBuffer(buf))
		if err != nil {
			return 0, nil, err
		}

		// Add authorisation token
		//phttp.SetToken(token, req)

		// SetToken adds the token to the request.
		//func SetToken(token string, req *http.Request) {
		req.Header.Set("Authorization", fmt.Sprintf("%s%s", "Token ", l.authToken))
		//}

		//if s.Gzip {
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
		//}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, nil, err
		}
		return resp.StatusCode, resp.Body, nil
	*/
}

func backpressurePred(body []byte) bool {
	if bytes.Contains(body, backoffMagicWords0) {
		return true
	} else if bytes.Contains(body, backoffMagicWords1) {
		return true
	} else if bytes.Contains(body, backoffMagicWords2a) && bytes.Contains(body, backoffMagicWords2b) {
		return true
	} else if bytes.Contains(body, backoffMagicWords3) {
		return true
	} else if bytes.Contains(body, backoffMagicWords4) {
		return true
	} else if bytes.Contains(body, backoffMagicWords5) {
		return true
	} else {
		return false
	}
}
