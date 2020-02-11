package utils

import (
	"strings"
)

// IsConnectionClosedError - checks the error to check if the connection was closed
func IsConnectionClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
