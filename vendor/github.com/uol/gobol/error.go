package gobol

// Error - defines a common http error interface
type Error interface {
	error
	StatusCode() int
	Message() string
	Package() string
	Function() string
	ErrorCode() string
}
