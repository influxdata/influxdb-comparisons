package main

import (
	"github.com/valyala/fasthttp"
	"sync"
	"time"
)

type HTTPClientPool struct {
	Pool 	*sync.Pool
	Host 	string
	Debug 	int
	Timeout time.Duration
}

func (p *HTTPClientPool) CachedOrNewHTTPClient() *HTTPClient {
	return p.Pool.Get().(*HTTPClient)
}

var ClientsPerHost = 512
var clientsPools map[string]*HTTPClientPool

func InitPool(urls []string, debug int, timeout time.Duration) {
	clientsPools = make(map[string]*HTTPClientPool)
	for i := 0; i < len(urls); i++ {
		hp := HTTPClientPool{
			Host: urls[i],
			Debug: debug,
			Timeout: timeout,
		}
		hp.Pool = &sync.Pool{
			New: func()interface{} {
				return NewHTTPClient(hp.Host, hp.Debug, hp.Timeout)
			},
		}
		for c := 0; c < ClientsPerHost; c++ {
			c := hp.Pool.New().(*HTTPClient)
			c.ping()
			hp.Pool.Put(c)
		}
		clientsPools[hp.Host] = &hp
	}
}

func (w *HTTPClient) ping() {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.Header.SetRequestURI(w.HostString + "/ping")
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	_ = w.client.Do(req, resp)
}

func CachedOrNewHTTPClient(host string, debug int, timeout time.Duration) *HTTPClient {
	return clientsPools[host].CachedOrNewHTTPClient()
}


