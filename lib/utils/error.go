package utils

import (
	"strings"

	"github.com/uol/mycenae/lib/constants"
)

const defaultValue string = "unknown"

// IsConnectionClosedError - checks the error to check if the connection was closed
func IsConnectionClosedError(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

// ValidateExpectedValue - validates a expected value
func ValidateExpectedValue(value string) string {
	if value == constants.StringsEmpty {
		return defaultValue
	}

	return value
}
