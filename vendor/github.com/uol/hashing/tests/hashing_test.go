package hashing_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/hashing"
)

const (
	testString = "hello world!"
)

// testResults - tests the results
func testResults(t *testing.T, algorithm hashing.Algorithm, expectedResult string, result []byte, err error) {

	if !assert.NoError(t, err, "error generating %s", algorithm) {
		return
	}

	assert.Equal(t, expectedResult, hex.EncodeToString(result), "unexpected %s result", algorithm)
}

// TestSHA256 - tests the sha256 implementation
func TestSHA256(t *testing.T) {

	expected := "7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9"
	algorithm := hashing.SHA256

	results, err := hashing.GenerateSHA256(testString)
	testResults(t, algorithm, expected, results, err)

	results, err = hashing.Generate(algorithm, testString)
	testResults(t, algorithm, expected, results, err)
}

// TestSHA1 - tests the sha1 implementation
func TestSHA1(t *testing.T) {

	expected := "430ce34d020724ed75a196dfc2ad67c77772d169"
	algorithm := hashing.SHA1

	results, err := hashing.GenerateSHA1(testString)
	testResults(t, algorithm, expected, results, err)

	results, err = hashing.Generate(algorithm, testString)
	testResults(t, algorithm, expected, results, err)
}

// TestCRC32 - tests the crc32 implementation
func TestCRC32(t *testing.T) {

	expected := "03b4c26d"
	algorithm := hashing.CRC32

	results, err := hashing.GenerateCRC32(testString)
	testResults(t, algorithm, expected, results, err)

	results, err = hashing.Generate(algorithm, testString)
	testResults(t, algorithm, expected, results, err)
}

// TestMD5 - tests the md5 implementation
func TestMD5(t *testing.T) {

	expected := "fc3ff98e8c6a0d3087d515c0473f8677"
	algorithm := hashing.MD5

	results, err := hashing.GenerateMD5(testString)
	testResults(t, algorithm, expected, results, err)

	results, err = hashing.Generate(algorithm, testString)
	testResults(t, algorithm, expected, results, err)
}

// TestShake256 - tests the shake 256 implementation
func TestShake256(t *testing.T) {

	size := []int{4, 8, 16, 32}
	expectedHash := []string{
		"1237cfe4",
		"1237cfe493413ac8",
		"1237cfe493413ac80f7b6b41369f7afa",
		"1237cfe493413ac80f7b6b41369f7afa4a3ada93e7edf8de9f93e476796f9aa1",
	}
	algorithm := hashing.SHAKE256

	for i := 0; i < len(size); i++ {

		results, err := hashing.GenerateSHAKE256(size[i], testString)
		testResults(t, algorithm, expectedHash[i], results, err)

		results, err = hashing.GenerateSHAKE(algorithm, size[i], testString)
		testResults(t, algorithm, expectedHash[i], results, err)
	}
}

// TestShake128 - tests the shake 128 implementation
func TestShake128(t *testing.T) {

	size := []int{4, 8, 16, 32}
	expectedHash := []string{
		"15372b0f",
		"15372b0f35229f5f",
		"15372b0f35229f5fa04f4a262efd609d",
		"15372b0f35229f5fa04f4a262efd609d79f9958d46f9693df968c821f6b2bfda",
	}
	algorithm := hashing.SHAKE128

	for i := 0; i < len(size); i++ {

		results, err := hashing.GenerateSHAKE128(size[i], testString)
		testResults(t, algorithm, expectedHash[i], results, err)

		results, err = hashing.GenerateSHAKE(algorithm, size[i], testString)
		testResults(t, algorithm, expectedHash[i], results, err)
	}
}
