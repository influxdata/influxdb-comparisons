package main

import "time"

const DefaultIdleConnectionTimeout = 90 * time.Second

var useFastHttp = true
var idleConnectionTimeout = DefaultIdleConnectionTimeout

// HTTPClient is a reusable HTTP Client.
type HTTPClientCommon struct {
	Host       []byte
	HostString string
	debug      int
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
}

// HTTPClient interface.
type HTTPClient interface {
	HostString() string
	Ping()
	Do(q *Query, opts *HTTPClientDoOptions) (lag float64, size int64, err error)
}

func NewHTTPClient(host string, debug int, dialTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) HTTPClient {
	if useFastHttp {
		return NewFastHTTPClient(host, debug, dialTimeout, readTimeout, writeTimeout)
	} else {
		return NewDefaultHTTPClient(host, debug, dialTimeout, readTimeout, writeTimeout)
	}
}

