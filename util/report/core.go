package report

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"crypto/tls"
	"github.com/valyala/fasthttp"
)

type valueKind byte

const (
	invalidValueKind valueKind = iota
	float64ValueKind
	int64ValueKind
	boolValueKind
)

// Tag represents an InfluxDB tag.
// Users should prefer to keep the strings as long-lived items.
type Tag struct {
	Key, Value string
}

func (t *Tag) Serialize(w io.Writer) {
	fmt.Fprintf(w, "%s=%s", t.Key, t.Value)
}

// Field represents an InfluxDB field.
// Users should prefer to keep the strings as long-lived items.
//
// Implementor's note: this type uses more memory than is ideal, but it avoids
// unsafe and it avoids reflection.
type Field struct {
	key          string
	int64Value   int64
	float64Value float64
	boolValue    bool
	mode         valueKind
}

func (f *Field) SetFloat64(key string, x float64) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.float64Value = x
	f.mode = float64ValueKind
}
func (f *Field) SetInt64(key string, x int64) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.int64Value = x
	f.mode = int64ValueKind
}
func (f *Field) SetBool(key string, x bool) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.boolValue = x
	f.mode = boolValueKind
}

func (f *Field) Serialize(w io.Writer) {
	if f.mode == int64ValueKind {
		fmt.Fprintf(w, "%s=%di", f.key, f.int64Value)
	} else if f.mode == float64ValueKind {
		fmt.Fprintf(w, "%s=%f", f.key, f.float64Value)
	} else {
		fmt.Fprintf(w, "%s=%v", f.key, f.boolValue)
	}
}

// Point wraps an InfluxDB point data.
// Its primary purpose is to be serialized out to a []byte.
// Users should prefer to keep the strings as long-lived items.
type Point struct {
	Measurement   string
	Tags          []Tag
	Fields        []Field
	TimestampNano int64
}

func (p *Point) Init(m string, ts int64) {
	p.Measurement = m
	p.TimestampNano = ts
}

func (p *Point) AddTag(k, v string) {
	p.Tags = append(p.Tags, Tag{Key: k, Value: v})
}

func (p *Point) AddInt64Field(k string, x int64) {
	f := Field{}
	f.SetInt64(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) AddIntField(k string, x int) {
	f := Field{}
	f.SetInt64(k, int64(x))
	p.Fields = append(p.Fields, f)
}

func (p *Point) AddBoolField(k string, x bool) {
	f := Field{}
	f.SetBool(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) AddFloat64Field(k string, x float64) {
	f := Field{}
	f.SetFloat64(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) Serialize(w io.Writer) {
	fmt.Fprintf(w, "%s", p.Measurement)
	for i, tag := range p.Tags {
		if i == 0 {
			fmt.Fprint(w, ",")
		}

		tag.Serialize(w)
		if i < len(p.Tags)-1 {
			fmt.Fprint(w, ",")
		}
	}
	for i, field := range p.Fields {
		if i == 0 {
			fmt.Fprint(w, " ")
		}

		field.Serialize(w)
		if i < len(p.Fields)-1 {
			fmt.Fprint(w, ",")
		}
	}
	if p.TimestampNano > 0 {
		fmt.Fprintf(w, " %d", p.TimestampNano)
	}
}

func (p *Point) Reset() {
	p.Measurement = ""
	p.Tags = p.Tags[:0]
	p.Fields = p.Fields[:0]
	p.TimestampNano = 0
}

var GlobalPointPool *sync.Pool = &sync.Pool{New: func() interface{} { return &Point{} }}

func GetPointFromGlobalPool() *Point {
	return GlobalPointPool.Get().(*Point)
}
func PutPointIntoGlobalPool(p *Point) {
	p.Reset()
	GlobalPointPool.Put(p)
}

type Collector struct {
	Points []*Point

	client           *fasthttp.Client
	writeUri         string
	baseUri          string
	encodedBasicAuth string
	dbName           string
	//1,2
	influxDBVersion int

	buf *bytes.Buffer
}

func NewCollector(influxhost, dbname, userName, password string) *Collector {
	encodedBasicAuth := ""
	if userName != "" {
		encodedBasicAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", userName, password)))
	}
	return &Collector{
		buf:    new(bytes.Buffer),
		Points: make([]*Point, 0, 0),
		client: &fasthttp.Client{
			Name: "collector",
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			MaxIdleConnDuration: 90 * time.Second,
		},
		baseUri:          influxhost,
		writeUri:         influxhost + "/write?db=" + url.QueryEscape(dbname),
		encodedBasicAuth: encodedBasicAuth,
		dbName:           dbname,
		influxDBVersion:  1,
	}
}

func NewCollectorV2(influxhost, orgId, bucketId, authToken string) *Collector {
	return &Collector{
		buf:    new(bytes.Buffer),
		Points: make([]*Point, 0, 0),
		client: &fasthttp.Client{
			Name: "collector",
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			MaxIdleConnDuration: 90 * time.Second,
		},
		baseUri:          influxhost,
		writeUri:         influxhost + "/api/v2/write?org=" + orgId + "&bucket=" + bucketId,
		encodedBasicAuth: authToken,
		dbName:           bucketId,
		influxDBVersion:  2,
	}
}

func (c *Collector) Put(p *Point) {
	c.Points = append(c.Points, p)
}

func (c *Collector) Reset() {
	c.Points = c.Points[:0]
	c.buf.Reset()
}

func (c *Collector) PrepBatch() {
	for _, p := range c.Points {
		p.Serialize(c.buf)
		fmt.Fprint(c.buf, "\n")
	}
}

func (c *Collector) CreateDatabase() error {
	var err error
	err = nil
	if c.influxDBVersion == 1 {
		req := fasthttp.AcquireRequest()
		req.Header.SetMethod("POST")
		req.Header.SetRequestURI(c.baseUri + "/query?q=create%20database%20" + url.QueryEscape(c.dbName))
		if c.encodedBasicAuth != "" {
			req.Header.Set("Authorization", c.encodedBasicAuth)
		}
		req.SetBody(c.buf.Bytes())

		// Perform the request while tracking latency:
		resp := fasthttp.AcquireResponse()
		err = c.client.Do(req, resp)

		if resp.StatusCode() != fasthttp.StatusOK {
			return fmt.Errorf("collector error: unexpected status code %d", resp.StatusCode())
		}

		fasthttp.ReleaseResponse(resp)
		fasthttp.ReleaseRequest(req)
	}
	return err
}

func (c *Collector) SendBatch() error {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethod("POST")
	req.Header.SetRequestURI(c.writeUri)
	if c.encodedBasicAuth != "" {
		if c.influxDBVersion == 1 {
			req.Header.Set("Authorization", c.encodedBasicAuth)
		} else {
			req.Header.Add("Authorization", fmt.Sprintf("Token %s", c.encodedBasicAuth))
		}
	}
	req.SetBody(c.buf.Bytes())

	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	err := c.client.Do(req, resp)

	if resp.StatusCode() != fasthttp.StatusNoContent && resp.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf("collector error: unexpected status code %d: %s", resp.StatusCode(), resp.Body())
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return err
}
