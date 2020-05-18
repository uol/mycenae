package opentsdb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/uol/serializer/serializer"
)

/**
* Has all serialization methods from the OpenTSDB serializer.
* @author rnojiri
**/

const (
	strPut            string = "put "
	strSpace          string = " "
	strEqual          string = "="
	byteLineSeparator byte   = 10
)

// SerializeGeneric - serializes with the correct cast based on the struct ArrayItem
func (s *Serializer) SerializeGeneric(item interface{}) (string, error) {

	if item == nil {
		return serializer.Empty, nil
	}

	casted, ok := item.(*ArrayItem)
	if !ok {
		return serializer.Empty, fmt.Errorf("unexpected instance type")
	}

	return s.Serialize(casted.Metric, casted.Timestamp, casted.Value, casted.Tags...)
}

// SerializeGenericArray - serializes with the correct cast based on the struct ArrayItem
func (s *Serializer) SerializeGenericArray(items ...interface{}) (string, error) {

	numItems := len(items)
	if numItems == 0 {
		return serializer.Empty, nil
	}

	casted := make([]*ArrayItem, numItems)

	var ok bool
	for i := 0; i < numItems; i++ {
		casted[i], ok = items[i].(*ArrayItem)
		if !ok {
			return serializer.Empty, fmt.Errorf("unexpected instance type on index: %d", i)
		}
	}

	return s.SerializeArray(casted...)
}

// SerializeArray - serializes an array of opentsdb data lines
func (s *Serializer) SerializeArray(items ...*ArrayItem) (string, error) {

	numItems := len(items)
	if numItems == 0 {
		return serializer.Empty, nil
	}

	var b strings.Builder
	b.Grow(s.bufferSize * numItems)

	var err error
	for i := 0; i < numItems; i++ {
		err = s.serializeLine(&b, items[i].Metric, items[i].Timestamp, items[i].Value, items[i].Tags...)
		if err != nil {
			return serializer.Empty, err
		}
	}

	return b.String(), nil
}

// Serialize - serializes an opentsdb data line
func (s *Serializer) Serialize(metric string, timestamp int64, value float64, tags ...interface{}) (string, error) {

	var b strings.Builder
	b.Grow(s.bufferSize)

	err := s.serializeLine(&b, metric, timestamp, value, tags...)
	if err != nil {
		return serializer.Empty, err
	}

	return b.String(), nil
}

// serializeLine - serializes an opentsdb data line (internal)
func (s *Serializer) serializeLine(b *strings.Builder, metric string, timestamp int64, value float64, tags ...interface{}) error {

	numTags := len(tags)

	if numTags%2 != 0 {
		return fmt.Errorf("the number of tags must be even")
	}

	b.WriteString(strPut)
	b.WriteString(metric)
	b.WriteString(strSpace)
	b.WriteString(strconv.FormatInt(timestamp, 10))
	b.WriteString(strSpace)
	b.WriteString(strconv.FormatFloat(value, serializer.ByteFloatFormat, -1, 64))
	b.WriteString(strSpace)

	for i := 0; i < numTags; i += 2 {

		key, ok := tags[i].(string)
		if !ok {
			return fmt.Errorf("error casting tag key to string")
		}

		value, err := s.writeValue(tags[i+1])
		if err != nil {
			return err
		}

		b.WriteString(key)
		b.WriteString(strEqual)
		b.WriteString(value)

		if i < numTags-2 {
			b.WriteString(strSpace)
		}
	}

	b.WriteByte(byteLineSeparator)

	return nil
}

// writeValue - returns the value from the reflected interface value
func (s *Serializer) writeValue(tagValue interface{}) (string, error) {

	value := reflect.ValueOf(tagValue)
	kind := value.Kind()

	switch kind {
	case reflect.String:
		return value.String(), nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8, reflect.Uintptr, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(value.Float(), serializer.ByteFloatFormat, -1, 64), nil
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), nil
	default:
		return serializer.Empty, fmt.Errorf("kind not mapped: %s", kind.String())
	}
}
