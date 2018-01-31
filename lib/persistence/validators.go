package persistence

import "regexp"

var reValidKey = regexp.MustCompile(`^[A-Za-z]{1}[0-9A-Za-z_]+$`)

// ValidateKey validates a keyspace id
func ValidateKey(ksid string) bool {
	return reValidKey.MatchString(ksid)
}
