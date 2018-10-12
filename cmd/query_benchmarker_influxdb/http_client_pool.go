package main

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"time"
)

type HTTPClientPool struct {
	Pool 			[]*HTTPClient
	Host 			string
	Debug 			int
	Timeout 		time.Duration
	Available		int
}

var clientsPools map[string]*HTTPClientPool

func (p *HTTPClientPool) CachedOrNewHTTPClient() *HTTPClient {
	var c *HTTPClient
	if p.Available > 0 {
		c = p.Pool[p.Available - 1]
		p.Available--
	} else {
		fmt.Printf("Pool [%v] depleted, creating new HTTPClient\n", p.Host)
		c = NewHTTPClient(p.Host, p.Debug, p.Timeout)
	}
	return c
}

func CachedOrNewHTTPClient(host string, debug int, timeout time.Duration) *HTTPClient {
	if clientsPools == nil {
		return NewHTTPClient(host, debug, timeout)
	}
	return clientsPools[host].CachedOrNewHTTPClient()
}

func InitPools(clientsPerHost int, urls []string, debug int, timeout time.Duration) {
	if clientsPerHost <= 0 {
		return
	}
	clientsPools = make(map[string]*HTTPClientPool)
	for i := 0; i < len(urls); i++ {
		fmt.Printf("Creating pool for %v...\n", urls[i])
		hp := HTTPClientPool{
			Host: urls[i],
			Debug: debug,
			Timeout: timeout,
		}
		hp.Pool = make([]*HTTPClient, clientsPerHost)
		for j := 0; j < clientsPerHost; j++ {
			c := NewHTTPClient(urls[i], debug, timeout)
			c.ping()
			hp.Pool[hp.Available] = c
			hp.Available++
			fmt.Printf(".")
		}
		fmt.Printf("\n")
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

