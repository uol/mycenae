package restrictedhttpclient

import (
	"errors"

	"github.com/uol/funks"
)

//
// Client configurations and validations.
// author: rnojiri
//

var (
	// ErrNullConfiguration - raised when the specified configuration is null
	ErrNullConfiguration error = errors.New("configuration is null")

	// ErrInvalidNumSimultaneousRequests - raised when the number of simultaneous requests is invalid
	ErrInvalidNumSimultaneousRequests error = errors.New("the number of simultaneous requests is invalid")

	// ErrInvalidRequestTimeout - raised when the request timeout is invalid
	ErrInvalidRequestTimeout error = errors.New("the request timeout is invalid")
)

// Configuration - the connection pool configuration
type Configuration struct {
	// MaxSimultaneousRequests - the maximum number of simultaneous running requests
	MaxSimultaneousRequests uint64 `json:"maxSimultaneousRequests"`

	// RequestTimeout - the maximum request time
	RequestTimeout funks.Duration `json:"requestTimeout"`

	// SkipCertificateValidation - enable/disable certificate validation check
	SkipCertificateValidation bool `json:"skipCertificateValidation"`
}

// Validate - validates the configuration
func (c *Configuration) Validate() error {

	if c == nil {
		return ErrNullConfiguration
	}

	if c.MaxSimultaneousRequests <= 0 {
		return ErrInvalidNumSimultaneousRequests
	}

	if c.RequestTimeout.Duration <= 0 {
		return ErrInvalidRequestTimeout
	}

	return nil
}
