package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pelletier/go-toml"
	"log"
	"math/rand"
	"reflect"
)

type Source interface{}

func GetSourceValue(s *Source, measurementName, itemKey string) interface{} {
	switch reflect.ValueOf(s).Kind() {
	case reflect.Array:
		array := (*s).([]interface{})
		return array[rand.Int63n(int64(len(array)))]
	case reflect.Map:
		log.Fatalf("generators are not supported (yet) ['%s/%s']", measurementName, itemKey)
	default:
		return *s
	}
	panic("unreachable")
}

type Tag struct {
	Name string
	Source Source
}

type Field struct {
	Count int
	Name string
	Source Source
}

type Measurement struct {
	Name string
	Sample float32
	Tags []Tag
	Fields []Field
}

type ExternalConfig struct {
	measurements []Measurement
}

var Config *ExternalConfig

func (c *ExternalConfig) String() string {
	var buf bytes.Buffer
	for _,m := range c.measurements {
		buf.WriteString(fmt.Sprintf("definition: %s, sample: %f\n", m.Name, m.Sample))
		buf.WriteString(fmt.Sprintf("  tags:\n"))
		for _,tag := range m.Tags {
			buf.WriteString(fmt.Sprintf("    tag: %s\n", tag.Name))
			buf.WriteString(fmt.Sprintf("      source: %v\n", tag.Source))
		}
		buf.WriteString(fmt.Sprintf("  fields:\n"))
		for _,field := range m.Fields {
			buf.WriteString(fmt.Sprintf("    field: %s, count: %d\n", field.Name, field.Count))
			buf.WriteString(fmt.Sprintf("      source: %v\n", field.Source))
		}
	}
	return buf.String()
}

func (c *ExternalConfig) GetTagBytesValue(measurementName, tagKey []byte, failIfNotFound bool) []byte {
	return []byte(c.GetTagValue(string(measurementName), string(tagKey), failIfNotFound))
}

func (c *ExternalConfig) GetTagValue(measurementName, tagKey string, failIfNotFound bool) string {
	for _,m := range c.measurements {
		if "" == measurementName || m.Name == measurementName {
			for _,tag := range m.Tags {
				if tag.Name == tagKey {
					return fmt.Sprintf("%v", GetSourceValue(&tag.Source, m.Name, tag.Name))
				}
			}
		}
	}
	if failIfNotFound {
		log.Fatalf("value for tag '%s/%s' not found", measurementName, tagKey)
	}
	return ""
}

func (c *ExternalConfig) GetFieldBytesValue(measurementName, tagKey []byte, failIfNotFound bool) interface{} {
	return c.GetFieldValue(string(measurementName), string(tagKey), failIfNotFound)
}

func (c *ExternalConfig) GetFieldValue(measurementName, fieldKey string, failIfNotFound bool) interface{} {
	for _,m := range c.measurements {
		if "" == measurementName || m.Name == measurementName {
			for _,field := range m.Fields {
				if field.Name == fieldKey {
					return GetSourceValue(&field.Source, m.Name, field.Name)
				}
			}
		}
	}
	if failIfNotFound {
		log.Fatalf("value for field '%s/%s' not found", measurementName, fieldKey)
	}
	return nil
}

func NewConfig(path string) (*ExternalConfig, error) {
	toml, err := toml.LoadFile(path)
	if err != nil {
		return nil, fmt.Errorf("file load failed: %v", err)
	}
	obj := toml.ToMap()["measurements"]
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshall failed: %v", err)
	}
	config := ExternalConfig{}
	err = json.Unmarshal(b, &config.measurements)
	if err != nil {
		return nil, fmt.Errorf("unmarshall failed: %v", err)
	}
	return &config, nil
}
