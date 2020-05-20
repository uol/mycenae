package funks

import (
	"crypto/tls"
	"net/http"
	"time"
)

/**
* Common functions used by http protocol.
* @author rnojiri
**/

// CreateHTTPClient - creates a new HTTP client
func CreateHTTPClient(timeout time.Duration, insecureSkipVerify bool) *http.Client {

	transportCore := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify},
	}

	httpClient := &http.Client{
		Transport: transportCore,
		Timeout:   timeout,
	}

	return httpClient
}
