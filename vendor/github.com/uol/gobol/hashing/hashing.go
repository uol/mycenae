package hashing

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"hash/crc32"
	"reflect"

	"golang.org/x/crypto/sha3"
)

/**
* Has hashing functions that produce byte array hashes.
* (no support for blake2b)
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

	// SHAKE128 - constant
	SHAKE128 Algorithm = "shake128"

	// SHAKE256 - constant
	SHAKE256 Algorithm = "shake256"

	// BLAKE2B384 - constant
	// BLAKE2B384 Algorithm = "blake2b384"

	// BLAKE2B256 - constant
	// BLAKE2B256 Algorithm = "blake2b256"

	// BLAKE2B512 - constant
	// BLAKE2B512 Algorithm = "blake2b512"
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

// generateShakeHash - the main process for shake hash
func generateShakeHash(h sha3.ShakeHash, outputSize int, parameters ...interface{}) ([]byte, error) {

	byteArray, err := GenerateByteArray(parameters...)
	if err != nil {
		return nil, err
	}

	_, err = h.Write(byteArray)
	if err != nil {
		return nil, err
	}

	output := make([]byte, outputSize)
	_, err = h.Read(output)
	if err != nil {
		return nil, err
	}

	return output, nil
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

// GenerateSHAKE128 - generates a shake128 hash based on the specified parameters
func GenerateSHAKE128(outputSize int, parameters ...interface{}) ([]byte, error) {

	return generateShakeHash(sha3.NewShake128(), outputSize, parameters...)
}

// GenerateSHAKE256 - generates a shake256 hash based on the specified parameters
func GenerateSHAKE256(outputSize int, parameters ...interface{}) ([]byte, error) {

	return generateShakeHash(sha3.NewShake256(), outputSize, parameters...)
}

// GenerateBlake2b - generates a blacke2b hash based on the specified parameters
// func GenerateBlake2b(blakeAlgorithm Algorithm, parameters ...interface{}) ([]byte, error) {

// 	var h hash.Hash
// 	var err error
// 	switch blakeAlgorithm {
// 	case BLAKE2B256:
// 		h, err = blake2b.New256(nil)
// 	case BLAKE2B512:
// 		h, err = blake2b.New512(nil)
// 	case BLAKE2B384:
// 		h, err = blake2b.New384(nil)
// 	default:
// 		err = fmt.Errorf("available sizes: 256, 384 and 512")
// 	}
// 	if err != nil {
// 		return nil, err
// 	}

// 	return generateHash(h, parameters...)
// }

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
	// case BLAKE2B256, BLAKE2B384, BLAKE2B512:
	// 	return GenerateBlake2b(algorithm, parameters)
	default:
		return nil, fmt.Errorf("no algorithm named %s", algorithm)
	}
}

// GenerateSHAKE - generates the shake hash using the selected algorithm
func GenerateSHAKE(algorithm Algorithm, outputSize int, parameters ...interface{}) ([]byte, error) {

	switch algorithm {
	case SHAKE128:
		return GenerateSHAKE128(outputSize, parameters...)
	case SHAKE256:
		return GenerateSHAKE256(outputSize, parameters...)
	default:
		return nil, fmt.Errorf("no algorithm named %s", algorithm)
	}
}
