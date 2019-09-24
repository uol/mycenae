package testerhttpserver_test

import (
	"net/http"
	"testing"

	"github.com/uol/gobol/tester/httpserver"

	"github.com/stretchr/testify/assert"
)

/**
* The tests for the http server used by tests.
* @author rnojiri
**/

// TestNoResponse - tests when no response configurations were found
func TestNoResponse(t *testing.T) {

	_, err := httpserver.NewHTTPServer("localhost", 18080, 5, nil)

	assert.Error(t, err, "expected an error")
}

// createDummyResponse - creates a dummy response data
func createDummyResponse() httpserver.ResponseData {

	headers := http.Header{}
	headers.Add("Content-type", "text/plain; charset=utf-8")

	return httpserver.ResponseData{
		RequestData: httpserver.RequestData{
			URI:     "/test",
			Body:    "test body",
			Method:  "GET",
			Headers: headers,
		},
		Status: http.StatusOK,
	}
}

// Test404 - tests when a non mapped response is called
func Test404(t *testing.T) {

	server := httpserver.CreateNewTestHTTPServer([]httpserver.ResponseData{createDummyResponse()})
	defer server.Close()

	response := httpserver.DoRequest(&httpserver.RequestData{
		URI:    "/not",
		Method: "GET",
	})

	assert.Equal(t, http.StatusNotFound, response.Status, "expected 404 status")

	response = httpserver.DoRequest(&httpserver.RequestData{
		URI:    "/test",
		Method: "POST",
	})

	assert.Equal(t, http.StatusNotFound, response.Status, "expected 404 status")

	response = httpserver.DoRequest(&httpserver.RequestData{
		URI:    "/test",
		Method: "GET",
	})

	assert.Equal(t, http.StatusOK, response.Status, "expected 200 status")
}

// TestSuccess - tests when everything goes right
func TestSuccess(t *testing.T) {

	configuredResponse := createDummyResponse()

	server := httpserver.CreateNewTestHTTPServer([]httpserver.ResponseData{configuredResponse})
	defer server.Close()

	reqHeader := http.Header{}
	reqHeader.Add("Content-type", "text/plain; charset=utf-8")

	clientRequest := &httpserver.RequestData{
		URI:     "/test",
		Body:    "test body",
		Method:  "GET",
		Headers: reqHeader,
	}

	serverResponse := httpserver.DoRequest(clientRequest)
	if !compareResponses(t, &configuredResponse, serverResponse) {
		return
	}

	serverRequest := httpserver.WaitForHTTPServerRequest(server)
	compareRequests(t, clientRequest, serverRequest)
}

// TestMultipleResponses - tests when everything goes right with multiple responses
func TestMultipleResponses(t *testing.T) {

	configuredResponse1 := createDummyResponse()
	configuredResponse1.URI = "/text"
	configuredResponse1.Method = "POST"

	configuredResponse2 := createDummyResponse()
	configuredResponse2.URI = "/json"
	configuredResponse2.Method = "PUT"
	configuredResponse2.Status = http.StatusCreated
	configuredResponse2.Body = `{"metric": "test-metric", "value": 1.0}`
	configuredResponse2.Headers.Del("Content-type")
	configuredResponse2.Headers.Set("Content-type", "application/json")

	server := httpserver.CreateNewTestHTTPServer([]httpserver.ResponseData{configuredResponse1, configuredResponse2})
	defer server.Close()

	reqHeader1 := http.Header{}
	reqHeader1.Set("Content-type", "text/plain; charset=utf-8")

	clientRequest1 := &httpserver.RequestData{
		URI:     "/text",
		Body:    "some text",
		Method:  "POST",
		Headers: reqHeader1,
	}

	serverResponse := httpserver.DoRequest(clientRequest1)
	if !compareResponses(t, &configuredResponse1, serverResponse) {
		return
	}

	serverRequest := httpserver.WaitForHTTPServerRequest(server)
	compareRequests(t, clientRequest1, serverRequest)

	reqHeader2 := http.Header{}
	reqHeader2.Set("Content-type", "application/json")

	clientRequest2 := &httpserver.RequestData{
		URI:     "/json",
		Body:    `{"metric": "test-metric", "value": 1.0}`,
		Method:  "PUT",
		Headers: reqHeader2,
	}

	serverResponse = httpserver.DoRequest(clientRequest2)
	if !compareResponses(t, &configuredResponse2, serverResponse) {
		return
	}

	serverRequest = httpserver.WaitForHTTPServerRequest(server)
	compareRequests(t, clientRequest2, serverRequest)
}

// compareResponses - compares two responses
func compareResponses(t *testing.T, r1 *httpserver.ResponseData, r2 *httpserver.ResponseData) bool {

	result := true

	result = result && assert.Equal(t, r1.Body, r2.Body, "same body expected")
	result = result && containsHeaders(t, r1.Headers, r2.Headers)
	result = result && assert.Equal(t, r1.Method, r2.Method, "same method expected")
	result = result && assert.Equal(t, r1.Status, r2.Status, "same status expected")
	result = result && assert.Equal(t, r1.URI, r2.URI, "same URI expected")

	return result
}

// compareRequests - compares two requests
func compareRequests(t *testing.T, r1 *httpserver.RequestData, r2 *httpserver.RequestData) bool {

	result := true

	result = result && assert.Equal(t, r1.Body, r2.Body, "same body expected")
	result = result && containsHeaders(t, r1.Headers, r2.Headers)
	result = result && assert.Equal(t, r1.Method, r2.Method, "same method expected")
	result = result && assert.Equal(t, r1.URI, r2.URI, "same URI expected")

	return result
}

// containsHeaders - checks for the headers
func containsHeaders(t *testing.T, mustExist, fullSet http.Header) bool {

	if mustExist == nil {
		return true
	}

	assert.NotNil(t, fullSet, "the full set of headers must not be null")

	for mustExistHeader, mustExistValues := range mustExist {

		if !assert.Truef(t, len(fullSet[mustExistHeader]) > 0, "expected a list of values for the header: %s", mustExistHeader) {
			return false
		}

		if !assert.Equal(t, fullSet[mustExistHeader], mustExistValues, "expected some headers") {
			return false
		}
	}

	return true
}
