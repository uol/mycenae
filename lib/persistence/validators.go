package persistence

import "regexp"

var reValidKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)

// ValidateKey validates a keyspace id
func ValidateKey(ksid string) bool {
	return reValidKey.MatchString(ksid)
}
