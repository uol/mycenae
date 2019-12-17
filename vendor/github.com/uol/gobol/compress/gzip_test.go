package compress_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/compress"
)

// TestZip - tests to zip an array of bytes
func TestZip(t *testing.T) {

	largeString := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus rutrum rutrum consequat. In hac habitasse platea dictumst. Suspendisse ornare diam lacus, non pretium sem molestie vel. Integer venenatis dictum magna non viverra. Aenean condimentum venenatis ipsum, ut elementum dolor bibendum sed. Donec pulvinar risus laoreet ante auctor sodales. Pellentesque ac purus volutpat, interdum ex ut, hendrerit dolor. Etiam aliquet non enim a eleifend.")

	compressed, err := compress.ZipBytes(largeString)
	if !assert.NoError(t, err, "error compressing bytes") {
		return
	}

	assert.True(t, len(largeString) > len(compressed), "expected less bytes than original string")
}

// TestZip - tests to zip an array of bytes and unzip it again
func TestUnzip(t *testing.T) {

	decompressedString := "Hello! This is a compressed text in gzip format, if you can see it, then it's decompressed!"

	compressedBytes, err := compress.ZipBytes([]byte(decompressedString))
	if !assert.NoError(t, err, "error compressing bytes") {
		return
	}

	decompressedBytes, err := compress.UnzipBytes(compressedBytes)
	if !assert.NoError(t, err, "error decompressing bytes") {
		return
	}

	assert.Equal(t, decompressedString, string(decompressedBytes), "expected the same phrase")
}
