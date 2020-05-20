package timeline_http_test

import (
	"net/http"
	"time"

	"github.com/uol/funks"
	"github.com/uol/gotest"
	serializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

const (
	testServerHost string = "localhost"
	testServerPort int    = 18080
)

// createTimeseriesBackend - creates a new test server simulating a timeseries backend
func createTimeseriesBackend() *gotest.HTTPServer {

	headers := http.Header{}
	headers.Add("Content-type", "application/json")

	responses := gotest.ResponseData{
		RequestData: gotest.RequestData{
			URI:     "/api/put",
			Method:  "PUT",
			Headers: headers,
		},
		Status: 201,
	}

	return gotest.CreateNewTestHTTPServer(
		testServerHost,
		testServerPort,
		[]gotest.ResponseData{responses},
	)
}

const (
	numberPoint = "numberJSON"
	textPoint   = "textJSON"
)

// createHTTPTransport - creates the http transport with custom batch send interval
func createHTTPTransport(transportBufferSize int, batchSendInterval time.Duration) *timeline.HTTPTransport {

	transportConf := timeline.HTTPTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			RequestTimeout: funks.Duration{
				Duration: time.Second,
			},
			BatchSendInterval: funks.Duration{
				Duration: batchSendInterval,
			},
			TransportBufferSize:  transportBufferSize,
			SerializerBufferSize: 256,
		},
		ServiceEndpoint:        "/api/put",
		Method:                 "PUT",
		ExpectedResponseStatus: 201,
		TimestampProperty:      "timestamp",
		ValueProperty:          "value",
	}

	transport, err := timeline.NewHTTPTransport(&transportConf)
	if err != nil {
		panic(err)
	}

	transport.AddJSONMapping(
		numberPoint,
		serializer.NumberPoint{},
		"metric",
		"value",
		"timestamp",
		"tags",
	)

	transport.AddJSONMapping(
		textPoint,
		serializer.TextPoint{},
		"metric",
		"text",
		"timestamp",
		"tags",
	)

	return transport
}

// newNumberPoint - creates a new number point
func newNumberPoint(value float64) *serializer.NumberPoint {

	return &serializer.NumberPoint{
		Point: serializer.Point{
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
func newTextPoint(text string) *serializer.TextPoint {

	return &serializer.TextPoint{
		Point: serializer.Point{
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

type ByMetric []serializer.NumberPoint

func (a ByMetric) Len() int           { return len(a) }
func (a ByMetric) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMetric) Less(i, j int) bool { return a[i].Metric < a[j].Metric }

type ByMetricP []*serializer.NumberPoint

func (a ByMetricP) Len() int           { return len(a) }
func (a ByMetricP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMetricP) Less(i, j int) bool { return a[i].Metric < a[j].Metric }
