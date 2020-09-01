package restrictedhttpclient

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/uol/funks"
)

//
// A simple wrapper to restrict the http requests number from a client.
// author: rnojiri
//

// Instance - a pool instance
type Instance struct {
	numRequests             uint64
	maxSimultaneousRequests uint64
	client                  *http.Client
}

// ErrMaxRequestsReached - raised when the maximum number of requests was reached
var ErrMaxRequestsReached error = errors.New("the maximum number of requests was reached")

// New - creates a new http connection pool
func New(configuration *Configuration) (*Instance, error) {

	err := configuration.Validate()
	if err != nil {
		return nil, err
	}

	return &Instance{
		numRequests:             0,
		maxSimultaneousRequests: configuration.MaxSimultaneousRequests,
		client:                  funks.CreateHTTPClient(configuration.RequestTimeout.Duration, configuration.SkipCertificateValidation),
	}, nil
}

// acquire - acquires a request
func (i *Instance) acquire() error {
	if atomic.LoadUint64(&i.numRequests) >= i.maxSimultaneousRequests {
		return ErrMaxRequestsReached
	}

	atomic.AddUint64(&i.numRequests, 1)
	return nil
}

// release - releases a request
func (i *Instance) release() {
	atomic.AddUint64(&i.numRequests, ^uint64(0))
}

// Get - wrapper for the Client.Get
func (i *Instance) Get(url string) (resp *http.Response, err error) {
	err = i.acquire()
	if err != nil {
		return
	}

	resp, err = i.client.Get(url)
	i.release()
	return
}

// Do - wrapper for the Client.Do
func (i *Instance) Do(req *http.Request) (resp *http.Response, err error) {

	err = i.acquire()
	if err != nil {
		return
	}

	resp, err = i.client.Do(req)
	i.release()
	return
}

// Post - wrapper for the Client.Post
func (i *Instance) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {

	err = i.acquire()
	if err != nil {
		return
	}

	resp, err = i.client.Post(url, contentType, body)
	i.release()
	return
}

// PostForm - wrapper for the Client.PostForm
func (i *Instance) PostForm(url string, data url.Values) (resp *http.Response, err error) {
	err = i.acquire()
	if err != nil {
		return
	}

	resp, err = i.client.PostForm(url, data)
	i.release()
	return
}

// Head - wrapper for the Client.Head
func (i *Instance) Head(url string) (resp *http.Response, err error) {
	err = i.acquire()
	if err != nil {
		return
	}

	resp, err = i.client.Head(url)
	i.release()
	return
}

// CloseIdleConnections - wrapper for the Client.CloseIdleConnections
func (i *Instance) CloseIdleConnections() {
	i.client.CloseIdleConnections()
}
