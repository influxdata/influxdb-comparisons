package http

import "time"

const DefaultIdleConnectionTimeout = 90 * time.Second

var UseFastHttp = true
var idleConnectionTimeout = DefaultIdleConnectionTimeout

// HTTPClient is a reusable HTTP Client.
type HTTPClientCommon struct {
	Host       []byte
	HostString string
	debug      int
}

// HTTPClientDoOptions wraps options uses when calling `Do`.
type HTTPClientDoOptions struct {
	Authorization		 string
	Debug                int
	PrettyPrintResponses bool
}

// HTTPClient interface.
type HTTPClient interface {
	HostString() string
	Do(q *Query, opts *HTTPClientDoOptions) (lag float64, err error)
}

func NewHTTPClient(host string, debug int, dialTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) HTTPClient {
	if UseFastHttp {
		return NewFastHTTPClient(host, debug, dialTimeout, readTimeout, writeTimeout)
	} else {
		return NewDefaultHTTPClient(host, debug, dialTimeout, readTimeout, writeTimeout)
	}
}

