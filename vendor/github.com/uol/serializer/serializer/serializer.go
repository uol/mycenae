package serializer

const (
	// Empty - defines an empty string
	Empty string = ""

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
