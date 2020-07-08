package serializer

import (
	"fmt"
	"reflect"
)

const (
	// Empty - defines an empty string
	Empty string = ""

	// Null - defines an null string
	Null string = "null"

	// ByteFloatFormat - defines the float format
	ByteFloatFormat byte = 'f'
)

// Serializer - a generic way to serialize
type Serializer interface {

	// SerializeGeneric - serializes with the correct cast based on the struct ArrayItem
	SerializeGeneric(item interface{}) (string, error)

	// SerializeGenericArray - serializes with the correct cast based on the struct ArrayItem
	SerializeGenericArray(item ...interface{}) (string, error)
}

// InterfaceHasZeroValue - is a value from a interface zero? (nil)
func InterfaceHasZeroValue(x interface{}) bool {

	if x == nil {
		return true
	}

	typeOf := reflect.TypeOf(x)

	if typeOf.Kind() != reflect.Interface {
		return false
	}

	return x == reflect.Zero(typeOf).Interface()
}

// PanicHandler - handles an unexpected panic
func PanicHandler() {
	if r := recover(); r != nil {
		fmt.Println("[critical error] unexpected error on serializer library:", r)
	}
}
