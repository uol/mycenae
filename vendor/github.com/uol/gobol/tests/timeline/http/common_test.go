package timeline_http_test

import (
	"net/http"
	"time"

	"github.com/uol/gobol/structs"
	"github.com/uol/gobol/tester/httpserver"
	"github.com/uol/gobol/timeline"
	"go.uber.org/zap"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimeseriesBackend - creates a new test server simulating a timeseries backend
func createTimeseriesBackend() *httpserver.HTTPServer {

	headers := http.Header{}
	headers.Add("Content-type", "application/json")

	responses := httpserver.ResponseData{
		RequestData: httpserver.RequestData{
			URI:     "/api/put",
			Method:  "PUT",
			Headers: headers,
		},
		Status: 201,
	}

	return httpserver.CreateNewTestHTTPServer([]httpserver.ResponseData{responses})
}

const (
	numberPoint = "numberJSON"
	textPoint   = "textJSON"
)

// createHTTPTransport - creates the http transport
func createHTTPTransport(logger *zap.Logger) *timeline.HTTPTransport {

	transportConf := timeline.HTTPTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			RequestTimeout:       time.Second,
			BatchSendInterval:    time.Second,
			TransportBufferSize:  1024,
			SerializerBufferSize: 5,
		},
		ServiceEndpoint:        "/api/put",
		Method:                 "PUT",
		ExpectedResponseStatus: 201,
		TimestampProperty:      "timestamp",
		ValueProperty:          "value",
	}

	transport, err := timeline.NewHTTPTransport(&transportConf, logger)
	if err != nil {
		panic(err)
	}

	transport.AddJSONMapping(
		numberPoint,
		structs.NumberPoint{},
		"metric",
		"value",
		"timestamp",
		"tags",
	)

	transport.AddJSONMapping(
		textPoint,
		structs.TextPoint{},
		"metric",
		"text",
		"timestamp",
		"tags",
	)

	return transport
}

// newNumberPoint - creates a new number point
func newNumberPoint(value float64) *structs.NumberPoint {

	return &structs.NumberPoint{
		Point: structs.Point{
			Metric:    "number-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "number",
				"customTag": "number-test",
			},
		},
		Value: value,
	}
}

// newTextPoint - creates a new text point
func newTextPoint(text string) *structs.TextPoint {

	return &structs.TextPoint{
		Point: structs.Point{
			Metric:    "text-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "text",
				"customTag": "text-test",
			},
		},
		Text: text,
	}
}
