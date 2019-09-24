package httpserver

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/uol/gobol/util"
)

const (
	// TestServerHost - the test server's hostname
	TestServerHost = "localhost"

	// TestServerPort - the test server's port
	TestServerPort = 18080

	maxRequestTimeout = 10
)

// CreateNewTestHTTPServer - creates a new server
func CreateNewTestHTTPServer(responses []ResponseData) *HTTPServer {

	s, err := NewHTTPServer(TestServerHost, TestServerPort, 5, responses)
	if err != nil {
		panic(err)
	}

	return s
}

// DoRequest - does a request
func DoRequest(request *RequestData) *ResponseData {

	client := util.CreateHTTPClient(time.Second, true)

	req, err := http.NewRequest(request.Method, fmt.Sprintf("http://%s:%d/%s", TestServerHost, TestServerPort, request.URI), bytes.NewBuffer([]byte(request.Body)))
	if err != nil {
		panic(err)
	}

	if len(request.Headers) > 0 {
		CopyHeaders(request.Headers, req.Header)
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	result, err := ParseResponse(res)
	if err != nil {
		panic(err)
	}

	result.URI = request.URI

	return result
}

// WaitForHTTPServerRequest - wait until timeout or for the server sets the request in the channel
func WaitForHTTPServerRequest(server *HTTPServer) *RequestData {

	var request *RequestData
	var seconds int

	fmt.Println("waiting")

	for {
		select {
		case request = <-server.RequestChannel():
			fmt.Println("received")
		default:
		}

		if request != nil {
			fmt.Println("not null")
			break
		}

		<-time.After(time.Second)
		seconds++

		if seconds >= maxRequestTimeout {
			fmt.Println("timeout")
			break
		}

		fmt.Println("loop")
	}

	fmt.Println("done")

	return request
}
