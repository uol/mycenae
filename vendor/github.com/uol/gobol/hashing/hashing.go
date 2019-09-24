package hashing

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/crc32"
	"reflect"
)

/**
* Has hashing functions that produce byte array hashes.
* @author rnojiri
**/

// Algorithm - the algorithm constant type
type Algorithm string

const (
	// SHA256 - constant
	SHA256 Algorithm = "sha256"

	// SHA1 - constant
	SHA1 Algorithm = "sha1"

	// MD5 - constant
	MD5 Algorithm = "md5"

	// CRC32 - constant
	CRC32 Algorithm = "crc32"
)

// GenerateByteArray - generates a new byte array based on the given parameters
func GenerateByteArray(parameters ...interface{}) ([]byte, error) {

	if len(parameters) == 0 {
		return nil, nil
	}

	result := []byte{}

	for _, p := range parameters {

		bytes, err := getByteArray(reflect.ValueOf(p))
		if err != nil {
			return nil, err
		}

		result = append(result, bytes...)
	}

	return result, nil
}

// generateHash - the main process
func generateHash(h hash.Hash, parameters ...interface{}) ([]byte, error) {

	byteArray, err := GenerateByteArray(parameters...)
	if err != nil {
		return nil, err
	}

	_, err = h.Write(byteArray)
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// GenerateSHA256 - generates a sha256 hash based on the specified parameters
func GenerateSHA256(parameters ...interface{}) ([]byte, error) {

	return generateHash(sha256.New(), parameters...)
}

// GenerateCRC32 - generates a sha256 hash based on the specified parameters
func GenerateCRC32(parameters ...interface{}) ([]byte, error) {

	return generateHash(crc32.NewIEEE(), parameters...)
}

// GenerateMD5 - generates a md5 hash based on the specified parameters
func GenerateMD5(parameters ...interface{}) ([]byte, error) {

	return generateHash(md5.New(), parameters...)
}

// GenerateSHA1 - generates a sha1 hash based on the specified parameters
func GenerateSHA1(parameters ...interface{}) ([]byte, error) {

	return generateHash(sha1.New(), parameters...)
}

// Generate - generates the hash using the selected algorithm
func Generate(algorithm Algorithm, parameters ...interface{}) ([]byte, error) {

	switch algorithm {
	case SHA256:
		return GenerateSHA256(parameters...)
	case SHA1:
		return GenerateSHA1(parameters...)
	case MD5:
		return GenerateMD5(parameters...)
	case CRC32:
		return GenerateCRC32(parameters...)
	default:
		return nil, fmt.Errorf("no algorithm named %s", algorithm)
	}
}
