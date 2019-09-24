package httpserver

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
)

// RequestData - the request data sent to the server
type RequestData struct {
	URI     string
	Body    string
	Method  string
	Headers http.Header
}

// ResponseData - the expected response data for each configured URI
type ResponseData struct {
	RequestData
	Status int
}

// HTTPServer - the server listening for HTTP requests
type HTTPServer struct {
	server         *httptest.Server
	requestChannel chan *RequestData
	responseMap    map[string]ResponseData
}

var multipleBarRegexp = regexp.MustCompile("[/]+")

// NewHTTPServer - creates a new HTTP listener server
func NewHTTPServer(host string, port, channelSize int, responses []ResponseData) (*HTTPServer, error) {

	if len(responses) == 0 {
		return nil, fmt.Errorf("expected at least one response")
	}

	hs := &HTTPServer{
		requestChannel: make(chan *RequestData, channelSize),
	}

	hs.responseMap = map[string]ResponseData{}
	for _, response := range responses {
		response.URI = CleanURI(response.URI)
		hs.responseMap[response.URI] = response
	}

	hs.server = httptest.NewUnstartedServer(http.HandlerFunc(hs.handler))

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	hs.server.Listener = listener
	hs.server.Start()

	return hs, nil
}

// CopyHeaders - copy all the headers
func CopyHeaders(source http.Header, dest http.Header) {

	if len(source) > 0 {
		for header, valueList := range source {
			for _, v := range valueList {
				dest.Set(header, v)
			}
		}
	}
}

// CleanURI - cleans and validates the URI
func CleanURI(name string) string {

	if !strings.HasPrefix(name, "/") {
		name += "/"
	}

	return multipleBarRegexp.ReplaceAllString(name, "/")
}

// handler - handles all requests
func (hl *HTTPServer) handler(res http.ResponseWriter, req *http.Request) {

	cleanURI := CleanURI(req.RequestURI)

	responseData, ok := hl.responseMap[cleanURI]
	if !ok || responseData.Method != req.Method {
		res.WriteHeader(http.StatusNotFound)
		return
	}

	combinedHeaders := res.Header()

	CopyHeaders(responseData.Headers, combinedHeaders)
	CopyHeaders(req.Header, combinedHeaders)

	res.WriteHeader(responseData.Status)

	if len(responseData.Body) > 0 {
		_, err := res.Write([]byte(responseData.Body))
		if err != nil {
			fmt.Println(fmt.Errorf("error writing response body: %s", err.Error()))
		}
	}

	bufferReqBody := new(bytes.Buffer)
	bufferReqBody.ReadFrom(req.Body)

	hl.requestChannel <- &RequestData{
		URI:     cleanURI,
		Body:    bufferReqBody.String(),
		Headers: req.Header,
		Method:  req.Method,
	}
}

// Close - closes this server
func (hl *HTTPServer) Close() {

	if hl.server != nil {
		hl.server.Close()
	}
}

// RequestChannel - reads from the request channel
func (hl *HTTPServer) RequestChannel() <-chan *RequestData {

	return hl.requestChannel
}

// ParseResponse - parses the response using the local struct as result
func ParseResponse(res *http.Response) (*ResponseData, error) {

	bufferReqBody := new(bytes.Buffer)
	_, err := bufferReqBody.ReadFrom(res.Body)
	if err != nil {
		return nil, err
	}

	return &ResponseData{
		RequestData: RequestData{
			URI:     res.Request.RequestURI,
			Body:    bufferReqBody.String(),
			Headers: res.Header,
			Method:  res.Request.Method,
		},
		Status: res.StatusCode,
	}, nil
}
