package hashing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/hashing"
)

/**
* The hashing library byte conversion tests.
* @author rnojiri
**/

// generateByteArray - function wrapper
func generateByteArray(t *testing.T, parameters ...interface{}) []byte {

	result, err := hashing.GenerateByteArray(parameters...)
	if !assert.NoError(t, err, "expected no error") {
		panic(err)
	}

	return result
}

// TestConvertBool - tests the bool to byte array conversion
func TestConvertBool(t *testing.T) {

	expected := []byte{1}
	assert.Equal(t, expected, generateByteArray(t, true), "expected %v", expected)

	expected = []byte{0}
	assert.Equal(t, expected, generateByteArray(t, false), "expected %v", expected)
}

// TestConvertInt - tests the int to byte array conversion
func TestConvertInt(t *testing.T) {

	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, generateByteArray(t, 0), "expected %v", expected)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 1}
	assert.Equal(t, expected, generateByteArray(t, 1), "expected %v", expected)

	expected = []byte{0, 0, 0, 0, 0, 0, 0, 0x10}
	assert.Equal(t, expected, generateByteArray(t, 16), "expected %v", expected)

	expected = []byte{0, 0, 0, 0, 0, 0, 0x04, 0x00}
	assert.Equal(t, expected, generateByteArray(t, 1024), "expected %v", expected)
}

// TestConvertFloat - tests the float to byte array conversion
func TestConvertFloat(t *testing.T) {

	expected := []byte{0x3f, 0xf0, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, generateByteArray(t, float64(1)), "expected %v", expected)

	expected = []byte{0x40, 0x17, 0, 0, 0, 0, 0, 0}
	assert.Equal(t, expected, generateByteArray(t, float64(5.75)), "expected %v", expected)
	assert.Equal(t, expected, generateByteArray(t, float32(5.75)), "expected %v", expected)

	expected = []byte{0xbf, 0xf1, 0xc7, 0x10, 0xcb, 0x29, 0x5e, 0x9e}
	assert.Equal(t, expected, generateByteArray(t, float64(-1.1111)), "expected %v", expected)
}

// TestConvertString - tests the string to byte array conversion
func TestConvertString(t *testing.T) {

	expected := []byte{0x31, 0x20, 0x32, 0x20, 0x33}
	assert.Equal(t, expected, generateByteArray(t, "1 2 3"), "expected %v", expected)

	expected = []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64}
	assert.Equal(t, expected, generateByteArray(t, "hello world"), "expected %v", expected)

	expected = []byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 0x60, 0x7b, 0x7c, 0x7d, 0x7e}
	assert.Equal(t, expected, generateByteArray(t, " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"), "expected %v", expected)
}

// TestConvertMap - tests the map to byte array conversion
func TestConvertMap(t *testing.T) {

	m1 := map[string]int{
		"z": 10,
		"d": 16,
		"a": 1001,
	}

	expected := []byte{0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x03, 0xE9, 0x64, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x7a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0A}
	assert.Equal(t, expected, generateByteArray(t, m1), "expected %v", expected)

	m2 := map[int]bool{
		3000: true,
		5:    false,
		100:  true,
	}

	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0B, 0xB8, 0x01}
	assert.Equal(t, expected, generateByteArray(t, m2), "expected %v", expected)
}

// TestConvertArray - tests the array to byte array conversion
func TestConvertArray(t *testing.T) {

	a1 := []string{"hello", "world", "!"}

	expected := []byte{0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21}
	assert.Equal(t, expected, generateByteArray(t, a1), "expected %v", expected)

	a2 := []int{5, 10, 1}

	expected = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xA, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}
	assert.Equal(t, expected, generateByteArray(t, a2), "expected %v", expected)

	a3 := []bool{true, false, true, false}

	expected = []byte{0x1, 0x0, 0x1, 0x0}
	assert.Equal(t, expected, generateByteArray(t, a3), "expected %v", expected)

	a4 := []float32{5.543, -6.1}

	expected = []byte{0x40, 0x16, 0x2c, 0x8, 0x40, 0x0, 0x0, 0x0, 0xc0, 0x18, 0x66, 0x66, 0x60, 0x0, 0x0, 0x0}
	assert.Equal(t, expected, generateByteArray(t, a4), "expected %v", expected)
}
