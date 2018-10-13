package main

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

// Method that rules them all
type HTTPClient interface {
	HostString() string
	Ping()
	Do(q *Query, opts *HTTPClientDoOptions) (lag float64, err error)
}

var UseFastHttp = true