package main

import (
	"fmt"
	"time"
)

type HTTPClientPool struct {
	Pool 			[]HTTPClient
	Host 			string
	Debug 			int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	Available		int
}

var clientsPools map[string]*HTTPClientPool

func (p *HTTPClientPool) CachedOrNewHTTPClient() HTTPClient {
	var c HTTPClient
	if p.Available > 0 {
		c = p.Pool[p.Available - 1]
		p.Available--
	} else {
		fmt.Printf("HTTP client pool [%v] depleted, creating new HTTPClient\n", p.Host)
		c = NewHTTPClient(p.Host, p.Debug, p.DialTimeout, p.ReadTimeout, p.WriteTimeout)
	}
	return c
}

func CachedOrNewHTTPClient(host string, debug int, dialTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) HTTPClient {
	if clientsPools == nil {
		return NewHTTPClient(host, debug, dialTimeout, readTimeout, writeTimeout)
	}
	return clientsPools[host].CachedOrNewHTTPClient()
}

func InitPools(clientsPerHost int, urls []string, debug int, dialTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) {
	if clientsPerHost <= 0 {
		return
	}
	clientsPools = make(map[string]*HTTPClientPool)
	for i := 0; i < len(urls); i++ {
		fmt.Printf("Creating HTTP client pool for %v ", urls[i])
		hp := HTTPClientPool{
			Host: urls[i],
			Debug: debug,
			DialTimeout: dialTimeout,
			ReadTimeout: readTimeout,
			WriteTimeout: writeTimeout,
		}
		hp.Pool = make([]HTTPClient, clientsPerHost)
		for j := 0; j < clientsPerHost; j++ {
			c := NewHTTPClient(urls[i], debug, dialTimeout, readTimeout, writeTimeout)
			c.Ping()
			hp.Pool[hp.Available] = c
			hp.Available++
			fmt.Printf(".")
		}
		fmt.Printf("\n")
		clientsPools[hp.Host] = &hp
	}
}