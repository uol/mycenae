package hashing

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
)

/**
* Has value to byte array conversion functions.
* @author rnojiri
**/

// float64ToByteArray - converts a float64 to byte array
func float64ToByteArray(f float64) []byte {

	var buffer = make([]byte, 8)

	binary.BigEndian.PutUint64(buffer, math.Float64bits(f))

	return buffer
}

// int64ToByteArray - converts a int64 to byte array
func int64ToByteArray(i int64) []byte {

	var buffer = make([]byte, 8)

	binary.BigEndian.PutUint64(buffer, uint64(i))

	return buffer
}

// uint64ToByteArray - converts a int64 to byte array
func uint64ToByteArray(i uint64) []byte {

	var buffer = make([]byte, 8)

	binary.BigEndian.PutUint64(buffer, i)

	return buffer
}

// boolToByteArray - converts a boolean to byte
func boolToByteArray(b bool) []byte {

	if b {
		return []byte{1}
	}

	return []byte{0}
}

// stringToByteArray - converts a string to byte array
func stringToByteArray(s string) []byte {

	if len(s) == 0 {
		return make([]byte, 0)
	}

	return []byte(s)
}

// mapToByteArray - converts a map to a byte array
func mapToByteArray(v reflect.Value) ([]byte, error) {

	if v.Len() == 0 {
		return nil, nil
	}

	var err error

	keys := v.MapKeys()
	sortKeys(keys, keys[0].Kind())

	var keyBytes, valBytes []byte
	bytes := []byte{}

	for _, k := range keys {

		keyBytes, err = getByteArray(k)
		if err != nil {
			return nil, err
		}

		valBytes, err = getByteArray(v.MapIndex(k))
		if err != nil {
			return nil, err
		}

		bytes = append(bytes, keyBytes...)
		bytes = append(bytes, valBytes...)
	}

	return bytes, nil
}

// arrayToByteArray - converts a array to a byte array
func arrayToByteArray(v reflect.Value) ([]byte, error) {

	var err error
	var indexBytes []byte
	bytes := []byte{}

	for i := 0; i < v.Len(); i++ {

		indexBytes, err = getByteArray(v.Index(i))
		if err != nil {
			return nil, err
		}

		bytes = append(bytes, indexBytes...)
	}

	return bytes, nil
}

// getByteArray - returns the byte array from the given value
func getByteArray(v reflect.Value) ([]byte, error) {

	switch v.Kind() {
	case reflect.String:
		return stringToByteArray(v.String()), nil
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		return int64ToByteArray(v.Int()), nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		return uint64ToByteArray(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return float64ToByteArray(v.Float()), nil
	case reflect.Bool:
		return boolToByteArray(v.Bool()), nil
	case reflect.Map:
		return mapToByteArray(v)
	case reflect.Array, reflect.Slice:
		return arrayToByteArray(v)
	default:
		return nil, fmt.Errorf("type is not mapped to get byte array: %s", v.Kind().String())
	}
}

// sortKeys - returns the sorted array for the specified type
func sortKeys(array []reflect.Value, kind reflect.Kind) error {

	switch kind {
	case reflect.String:
		sort.Sort(byString(array))
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		sort.Sort(byInt(array))
	case reflect.Float32, reflect.Float64:
		sort.Sort(byFloat(array))
	case reflect.Bool:
		sort.Sort(byBool(array))
	default:
		return fmt.Errorf("type is not mapped to sort keys: %s", kind.String())
	}

	return nil
}
