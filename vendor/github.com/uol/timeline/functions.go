package timeline

import (
	"encoding/hex"
	"strings"

	"github.com/uol/hashing"
)

const (
	shake = "shake"
	empty = ""
)

// getHash - returns the hash
func getHash(config *DataTransformerConfig, hashParameters ...interface{}) (string, error) {

	var hashBytes []byte
	var err error

	if config.isSHAKE {
		hashBytes, err = hashing.GenerateSHAKE(config.HashingAlgorithm, config.HashSize, hashParameters...)
	} else {
		hashBytes, err = hashing.Generate(config.HashingAlgorithm, hashParameters...)
	}

	if err != nil {
		return empty, err
	}

	return hex.EncodeToString(hashBytes), nil
}

// isShakeAlgorithm - check if it is SHAKE
func isShakeAlgorithm(algorithm hashing.Algorithm) bool {

	return strings.HasPrefix(string(algorithm), shake)
}
