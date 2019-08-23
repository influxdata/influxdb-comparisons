// Incomplete support for schema description and data generation
// using TOML format as supported by influx_tools in branch 1.7+

package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pelletier/go-toml"
	"log"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
)

type Source interface{}

var DefaultValueGenerator = map[string]interface{}{
	"type": "default",
}

func getSourceValue(s *Source, measurementName, itemKey string, itemDefaultValue interface{}) interface{} {
	switch reflect.Indirect(reflect.ValueOf(s)).Elem().Kind() {
	case reflect.Array:
		array := (*s).([]interface{})
		return array[rand.Int63n(int64(len(array)))]
	case reflect.Map:
		m := (*s).(map[string]interface{})
		if reflect.DeepEqual(m, DefaultValueGenerator) {
			return itemDefaultValue
		}
		log.Fatalf("generators are not supported (yet) ['%s/%s']", measurementName, itemKey)
	default: // primitive types
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

func (c *ExternalConfig) GetTagBytesValue(measurementName, tagKey []byte, failIfNotFound bool, defaultValue []byte) []byte {
	return []byte(c.GetTagValue(string(measurementName), string(tagKey), failIfNotFound, string(defaultValue)))
}

func (c *ExternalConfig) GetTagValue(measurementName, tagKey string, failIfNotFound bool, defaultValue string) string {
	for _,m := range c.measurements {
		if "" == measurementName || m.Name == measurementName {
			for _,tag := range m.Tags {
				if tag.Name == tagKey {
					return fmt.Sprintf("%v", getSourceValue(&tag.Source, m.Name, tag.Name, defaultValue))
				}
			}
		}
	}
	if failIfNotFound {
		log.Fatalf("value for tag '%s/%s' not found", measurementName, tagKey)
	}
	return "" // defaultValue ?
}

func (c *ExternalConfig) GetFieldBytesValue(measurementName, tagKey []byte, failIfNotFound bool, defaultValue interface{}) interface{} {
	return c.GetFieldValue(string(measurementName), string(tagKey), failIfNotFound, defaultValue)
}

func (c *ExternalConfig) GetFieldValue(measurementName, fieldKey string, failIfNotFound bool, defaultValue interface{}) interface{} {
	for _,m := range c.measurements {
		if "" == measurementName || m.Name == measurementName {
			for _,field := range m.Fields {
				if field.Name == fieldKey {
					return getSourceValue(&field.Source, m.Name, field.Name, defaultValue)
				}
			}
		}
	}
	if failIfNotFound {
		log.Fatalf("value for field '%s/%s' not found", measurementName, fieldKey)
	}
	return nil // defaultValue ?
}

func NewConfig(path string) (*ExternalConfig, error) {
	var tree *toml.Tree
	var err error
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path,"https://") {
		tree, err = LoadURL(path)
	} else {
		tree, err = toml.LoadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("config loading failed: %v", err)
	}
	obj := tree.ToMap()["measurements"]
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("config marshall failed: %v", err)
	}
	config := ExternalConfig{}
	err = json.Unmarshal(b, &config.measurements)
	if err != nil {
		return nil, fmt.Errorf("config unmarshall failed: %v", err)
	}
	return &config, nil
}

// LoadURL creates a Tree from a URL resource.
func LoadURL(url string) (tree *toml.Tree, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("config loading failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		tree, err := toml.LoadReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("config parsing failed: %v", err)
		}
		return tree, nil
	}
	return nil, fmt.Errorf("config loading failed: response status code is: %s", resp.Status)
}
