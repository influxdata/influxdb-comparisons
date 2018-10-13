package main

import (
	"fmt"
	"time"
)

type HTTPClientPool struct {
	Pool 			[]HTTPClient
	Host 			string
	Debug 			int
	Timeout 		time.Duration
	Available		int
}

var UseFastHttp = true

var clientsPools map[string]*HTTPClientPool

func (p *HTTPClientPool) CachedOrNewHTTPClient() HTTPClient {
	var c HTTPClient
	if p.Available > 0 {
		c = p.Pool[p.Available - 1]
		p.Available--
	} else {
		fmt.Printf("HTTP client pool [%v] depleted, creating new HTTPClient\n", p.Host)
		c = NewHTTPClient(p.Host, p.Debug, p.Timeout)
	}
	return c
}

func CachedOrNewHTTPClient(host string, debug int, timeout time.Duration) HTTPClient {
	if clientsPools == nil {
		return NewHTTPClient(host, debug, timeout)
	}
	return clientsPools[host].CachedOrNewHTTPClient()
}

func NewHTTPClient(host string, debug int, timeout time.Duration) HTTPClient {
	if UseFastHttp {
		return NewFastHTTPClient(host, debug, timeout)
	} else {
		return NewGoHTTPClient(host, debug, timeout)
	}
}

func InitPools(clientsPerHost int, urls []string, debug int, timeout time.Duration) {
	if clientsPerHost <= 0 {
		return
	}
	clientsPools = make(map[string]*HTTPClientPool)
	for i := 0; i < len(urls); i++ {
		fmt.Printf("Creating HTTP client pool for %v ", urls[i])
		hp := HTTPClientPool{
			Host: urls[i],
			Debug: debug,
			Timeout: timeout,
		}
		hp.Pool = make([]HTTPClient, clientsPerHost)
		for j := 0; j < clientsPerHost; j++ {
			c := NewHTTPClient(urls[i], debug, timeout)
			c.Ping()
			hp.Pool[hp.Available] = c
			hp.Available++
			fmt.Printf(".")
		}
		fmt.Printf("\n")
		clientsPools[hp.Host] = &hp
	}
}