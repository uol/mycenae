package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var metric1 string
var metric2 string
var tagApp string
var tagApp1 string
var tagHost string
var tagHost1 string
var tagHost2 string
var tagHost3 string
var tagService string
var tagService1 string
var tagService2 string

func sendPointsParseExp(keyset string) {

	fmt.Println("Setting up parseExpression_test.go tests...")

	metric1 = "testParseExpression"
	metric2 = "testParseExpression2"
	tagApp = "app"
	tagApp1 = "app1"
	tagHost = "host"
	tagHost1 = "host1"
	tagHost2 = "host2"
	tagHost3 = "host3"
	tagService = "service"
	tagService1 = "service1"
	tagService2 = "service2"

	points := `[
	  {
		"value": 36.5,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keyset + `",
		  "` + tagHost + `":"` + tagHost1 + `"
		}
	  },
	  {
		"value": 54.5,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost + `":"` + tagHost2 + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 5.4,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost + `":"` + tagHost3 + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagService + `":"` + tagService1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagService + `":"` + tagService1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagService + `":"` + tagService1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagService + `":"` + tagService2 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagService + `":"` + tagService2 + `"
		}
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keyset + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagService + `":"` + tagService2 + `"
		}
	  }
	]`

	code, _, err := mycenaeTools.HTTP.POST("api/put", []byte(points))
	if err != nil {
		panic(err)
	}
	if code != http.StatusNoContent {
		log.Fatal("Error sending points!")
	}
}

// HELPERS

func parseExp(t *testing.T, urlQuery string) (int, []tools.TSDBqueryPayload) {

	status, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("expression/parse?%s", urlQuery))

	response := []tools.TSDBqueryPayload{}

	err := json.Unmarshal(resp, &response)
	if err != nil {
		t.Error(err, string(resp))
		t.SkipNow()
	}

	return status, response
}

func parseAssertInvalidExp(t *testing.T, path, test, err, msg string) {

	status, resp, _ := mycenaeTools.HTTP.GET(path)
	response := tools.Error{}

	errJSON := json.Unmarshal(resp, &response)
	if errJSON != nil {
		t.Error(errJSON, test)
		t.SkipNow()
	}

	assert.Equal(t, 400, status, test)
	assert.Equal(t, err, response.Error, test)
	assert.Equal(t, msg, response.Message, test)
}

// TESTS

func TestParseValidQueryMetricAndTagsWithSpecialChars(t *testing.T) {

	expression := url.QueryEscape(
		"merge(sum, downsample(30s, min,none, query(os-%&#;_/.cpu, {ap-%&#;_/.p=tes-%&#;_/.t}, 5m)))")

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "30s-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os-%&#;_/.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, "ap-%&#;_/.p", response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "tes-%&#;_/.t", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleSec(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(30s, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "30s-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleHours(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(2h, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "2h-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleDays(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1d, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1d-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleWeeks(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(3w, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "3w-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleMonths(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(2n, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "2n-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleYear(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1y, min,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1y-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleMax(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, max,none, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-max-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleAvg(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, avg, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-avg-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleSum(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleCount(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, count, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-count-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFillNull(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, null,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-null", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFillNan(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, nan,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-nan", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFillZero(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, zero,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-zero", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFilterGreaterThan(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, none,filter(>5, query(os.cpu, {app=nonexistent}, 5m))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[2])
	assert.Equal(t, ">5", response[0].Queries[0].FilterValue)
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFilterGreaterThanEqualTo(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, none,filter(>=5, query(os.cpu, {app=nonexistent}, 5m))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[2])
	assert.Equal(t, ">=5", response[0].Queries[0].FilterValue)
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFilterLessThan(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, none,filter(<5, query(os.cpu, {app=nonexistent}, 5m))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[2])
	assert.Equal(t, "<5", response[0].Queries[0].FilterValue)
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryDownsampleFilterLessThanEqualTo(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, sum, none,filter(<=5, query(os.cpu, {app=nonexistent}, 5m))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[2])
	assert.Equal(t, "<=5", response[0].Queries[0].FilterValue)
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMergeMax(t *testing.T) {

	expression := url.QueryEscape(
		`merge(max, downsample(1m, sum, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "max", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMergeAvg(t *testing.T) {

	expression := url.QueryEscape(
		`merge(avg, downsample(1m, sum, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "avg", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMergeMin(t *testing.T) {

	expression := url.QueryEscape(
		`merge(min, downsample(1m, sum, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "min", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMergeCount(t *testing.T) {

	expression := url.QueryEscape(
		`merge(count, downsample(1m, sum, none,query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "count", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-sum-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeSec(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 30s)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "30s", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeHour(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 2h)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "2h", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeDay(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 1d)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "1d", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeWeek(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 3w)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "3w", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeMonth(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 2n)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "2n", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryRelativeYear(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 1y)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "1y", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMoreThanOneTag(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent, host=host1, cpu=1}, 5m)))`)

	filter1 := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "app",
		Filter:  "nonexistent",
		GroupBy: false,
	}

	filter2 := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "host",
		Filter:  "host1",
		GroupBy: false,
	}

	filter3 := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "cpu",
		Filter:  "1",
		GroupBy: false,
	}

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 3, len(response[0].Queries[0].Filters))
	assert.Contains(t, response[0].Queries[0].Filters, filter1)
	assert.Contains(t, response[0].Queries[0].Filters, filter2)
	assert.Contains(t, response[0].Queries[0].Filters, filter3)

}

func TestParseValidQueryNoTags(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(os.cpu, null, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 0, len(response[0].Queries[0].Filters))

}

func TestParseValidQuerySameTagkExpandTrue(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(testParseExpression2, {host2=*, host2=host3}, 5m)))`)

	query := fmt.Sprintf("expand=true&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric2, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 2, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagHost2, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "*", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)
	assert.Equal(t, tagHost2, response[0].Queries[0].Filters[1].Tagk)
	assert.Equal(t, tagHost3, response[0].Queries[0].Filters[1].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[1].GroupBy)

}

func TestParseValidQuerySameTagkExpandFalse(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, downsample(1m, min, none, query(testParseExpression2, {host2=*, host2=host3}, 5m)))`)

	filter1 := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "host2",
		Filter:  "*",
		GroupBy: false,
	}

	filter2 := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "host2",
		Filter:  "host3",
		GroupBy: false,
	}

	query := fmt.Sprintf("expand=false&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric2, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 2, len(response[0].Queries[0].Filters))
	assert.Contains(t, response[0].Queries[0].Filters, filter1)
	assert.Contains(t, response[0].Queries[0].Filters, filter2)

}

func TestParseValidQueryOrderDownsampleMerge(t *testing.T) {

	expression := url.QueryEscape(
		`downsample(1m, min, none, merge(sum, query(os.cpu, {app=nonexistent}, 5m)))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryOrderMergeDownsampleRate(t *testing.T) {

	expression := url.QueryEscape(
		`rate(false,null,0,downsample(1m,min,none,merge(sum,query(os.cpu,{app=nonexistent},5m))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[0])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[1])
	assert.Equal(t, "rate", response[0].Queries[0].Order[2])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryOrderMergeRateDownsampleFilterValue(t *testing.T) {

	expression := url.QueryEscape(
		`filter(>5, downsample(1m, min, none, rate(false, null, 0, merge(min, query(os.cpu, {app=nonexistent}, 5m)))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "min", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 4, len(response[0].Queries[0].Order))
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[0])
	assert.Equal(t, "rate", response[0].Queries[0].Order[1])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[2])
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[3])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, ">5", response[0].Queries[0].FilterValue)
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryOrderFilterValueRateMergeDownsample(t *testing.T) {

	expression := url.QueryEscape(
		`downsample(1m, min, none, merge(min, rate(false, null, 0, filter(==5, query(os.cpu, {app=nonexistent}, 5m)))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "min", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 4, len(response[0].Queries[0].Order))
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[0])
	assert.Equal(t, "rate", response[0].Queries[0].Order[1])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[2])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[3])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "==5", response[0].Queries[0].FilterValue)
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryOrderRateFilterValueDownsampleMerge(t *testing.T) {

	expression := url.QueryEscape(
		`merge(count, downsample(1m, min, null, filter(<= 5, rate(true, 1, 2, query(os.cpu, {app=nonexistent}, 5m)))))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "count", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-null", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, true, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, int64(1), *response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(2), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 4, len(response[0].Queries[0].Order))
	assert.Equal(t, "rate", response[0].Queries[0].Order[0])
	assert.Equal(t, "filterValue", response[0].Queries[0].Order[1])
	assert.Equal(t, "downsample", response[0].Queries[0].Order[2])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[3])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "<=5", response[0].Queries[0].FilterValue)
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryMergeWithoutDownsample(t *testing.T) {

	expression := url.QueryEscape(
		`merge(sum, query(os.cpu, {app=nonexistent}, 5m))`)

	status, response := parseExp(t, fmt.Sprintf("exp=%s", expression))

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "", response[0].Queries[0].Downsample)
	assert.Equal(t, "os.cpu", response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 1, len(response[0].Queries[0].Order))
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[0])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagApp, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "nonexistent", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryGroupbyExpandMatchNoTags(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(testParseExpression, null, 5m))))`)

	filters := []string{tagHost1, tagHost2, tagHost3}

	query := fmt.Sprintf("exp=%s&expand=true&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 3, len(response))

	for i := 0; i < len(response); i++ {

		assert.Equal(t, 1, len(response[i].Queries))
		assert.Equal(t, "5m", response[i].Relative)
		assert.Equal(t, "sum", response[i].Queries[0].Aggregator)
		assert.Equal(t, "1m-min-none", response[i].Queries[0].Downsample)
		assert.Equal(t, metric1, response[i].Queries[0].Metric)
		assert.Equal(t, 0, len(response[i].Queries[0].Tags))
		assert.Equal(t, true, response[i].Queries[0].RateOptions.Counter)
		assert.Equal(t, (*int64)(nil), response[i].Queries[0].RateOptions.CounterMax)
		assert.Equal(t, int64(0), response[i].Queries[0].RateOptions.ResetValue)
		assert.Equal(t, 3, len(response[i].Queries[0].Order))
		assert.Equal(t, "downsample", response[i].Queries[0].Order[0])
		assert.Equal(t, "aggregation", response[i].Queries[0].Order[1])
		assert.Equal(t, "rate", response[i].Queries[0].Order[2])
		assert.Equal(t, 1, len(response[i].Queries[0].Filters))
		assert.Equal(t, "wildcard", response[i].Queries[0].Filters[0].Ftype)
		assert.Equal(t, tagHost, response[i].Queries[0].Filters[0].Tagk)
		assert.Equal(t, false, response[i].Queries[0].Filters[0].GroupBy)
		assert.Contains(t, filters, response[i].Queries[0].Filters[0].Filter)
	}

}

func TestParseValidQueryGroupbyExpandMatchWithTags(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(testParseExpression2, {app=app1}, 5m))))`)

	filterApp := tools.TSDBfilter{
		Ftype:   "wildcard",
		Tagk:    "app",
		Filter:  "app1",
		GroupBy: false,
	}

	filtersGB := []tools.TSDBfilter{
		{
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host1",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host2",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host3",
			GroupBy: false,
		},
	}

	query := fmt.Sprintf("exp=%s&expand=true&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 3, len(response))

	for i := 0; i < len(response); i++ {

		assert.Equal(t, 1, len(response[i].Queries))
		assert.Equal(t, "5m", response[i].Relative)
		assert.Equal(t, "sum", response[i].Queries[0].Aggregator)
		assert.Equal(t, "1m-min-none", response[i].Queries[0].Downsample)
		assert.Equal(t, metric2, response[i].Queries[0].Metric)
		assert.Equal(t, 0, len(response[i].Queries[0].Tags))
		assert.Equal(t, true, response[i].Queries[0].RateOptions.Counter)
		assert.Equal(t, (*int64)(nil), response[i].Queries[0].RateOptions.CounterMax)
		assert.Equal(t, int64(0), response[i].Queries[0].RateOptions.ResetValue)
		assert.Equal(t, 3, len(response[i].Queries[0].Order))
		assert.Equal(t, "downsample", response[i].Queries[0].Order[0])
		assert.Equal(t, "aggregation", response[i].Queries[0].Order[1])
		assert.Equal(t, "rate", response[i].Queries[0].Order[2])
		assert.Equal(t, 2, len(response[i].Queries[0].Filters))
		assert.Contains(t, response[i].Queries[0].Filters, filterApp)

		for j, filter := range response[i].Queries[0].Filters {

			if filter == filterApp {

				if j == 0 {
					assert.Contains(t, filtersGB, response[i].Queries[0].Filters[1])
				} else {
					assert.Contains(t, filtersGB, response[i].Queries[0].Filters[0])
				}
				break
			}
		}
	}
}

func TestParseValidQueryGroupbyExpandDontMatch(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(testParseExpression, {app=nonexistent}, 5m))))`)

	status, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf("expression/parse?exp=%s&expand=true&ksid=%s", expression, ksMycenae))
	assert.Equal(t, 204, status)
	assert.Equal(t, []byte{}, resp)

}

func TestParseValidQueryGroupbyExpandFalse(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(testParseExpression, null, 5m))))`)

	query := fmt.Sprintf("exp=%s&expand=false&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric1, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, true, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, "rate", response[0].Queries[0].Order[2])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagHost, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "*", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, true, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryGroupbyWithoutExpand(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(testParseExpression, null, 5m))))`)

	query := fmt.Sprintf("exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric1, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, true, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 3, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, "rate", response[0].Queries[0].Order[2])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagHost, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, "*", response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, true, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryGroupbWithoutRate(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|merge(sum, downsample(1m, min, none, query(testParseExpression, null, 5m)))`)

	filtersGB := []tools.TSDBfilter{
		{
			Ftype:   "wildcard",
			Tagk:    "host",
			Filter:  "host1",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host",
			Filter:  "host2",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host",
			Filter:  "host3",
			GroupBy: false,
		},
	}

	query := fmt.Sprintf("exp=%s&expand=true&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 3, len(response))

	for i := 0; i < len(response); i++ {

		assert.Equal(t, 1, len(response[i].Queries))
		assert.Equal(t, "5m", response[i].Relative)
		assert.Equal(t, "sum", response[i].Queries[0].Aggregator)
		assert.Equal(t, "1m-min-none", response[i].Queries[0].Downsample)
		assert.Equal(t, metric1, response[i].Queries[0].Metric)
		assert.Equal(t, 0, len(response[i].Queries[0].Tags))
		assert.Equal(t, false, response[i].Queries[0].RateOptions.Counter)
		assert.Equal(t, (*int64)(nil), response[i].Queries[0].RateOptions.CounterMax)
		assert.Equal(t, int64(0), response[i].Queries[0].RateOptions.ResetValue)
		assert.Equal(t, 2, len(response[i].Queries[0].Order))
		assert.Equal(t, "downsample", response[i].Queries[0].Order[0])
		assert.Equal(t, "aggregation", response[i].Queries[0].Order[1])
		assert.Equal(t, 1, len(response[i].Queries[0].Filters))
		assert.Contains(t, filtersGB, response[i].Queries[0].Filters[0])
	}

}

func TestParseValidQueryGroupbyTwoTags(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host2=*, service=*})|merge(sum, downsample(1m, min, none, query(testParseExpression2, null, 5m)))`)

	filtersHost := []tools.TSDBfilter{
		{
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host1",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host2",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host3",
			GroupBy: false,
		},
	}

	filtersService := []tools.TSDBfilter{
		{
			Ftype:   "wildcard",
			Tagk:    "service",
			Filter:  "service1",
			GroupBy: false,
		}, {
			Ftype:   "wildcard",
			Tagk:    "service",
			Filter:  "service2",
			GroupBy: false,
		},
	}

	query := fmt.Sprintf("expand=true&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 6, len(response))

	for i := 0; i < 6; i++ {

		assert.Equal(t, 1, len(response[i].Queries))
		assert.Equal(t, "5m", response[i].Relative)
		assert.Equal(t, "sum", response[i].Queries[0].Aggregator)
		assert.Equal(t, "1m-min-none", response[i].Queries[0].Downsample)
		assert.Equal(t, metric2, response[0].Queries[0].Metric)
		assert.Equal(t, 0, len(response[i].Queries[0].Tags))
		assert.Equal(t, false, response[i].Queries[0].RateOptions.Counter)
		assert.Equal(t, (*int64)(nil), response[i].Queries[0].RateOptions.CounterMax)
		assert.Equal(t, int64(0), response[i].Queries[0].RateOptions.ResetValue)
		assert.Equal(t, 2, len(response[i].Queries[0].Order))
		assert.Equal(t, "downsample", response[i].Queries[0].Order[0])
		assert.Equal(t, "aggregation", response[i].Queries[0].Order[1])
		assert.Equal(t, 2, len(response[i].Queries[0].Filters))

		if response[i].Queries[0].Filters[0].Tagk == tagHost2 {
			assert.Contains(t, filtersHost, response[i].Queries[0].Filters[0])
			assert.Contains(t, filtersService, response[i].Queries[0].Filters[1])
		} else {
			assert.Contains(t, filtersHost, response[i].Queries[0].Filters[1])
			assert.Contains(t, filtersService, response[i].Queries[0].Filters[0])
		}
	}

}

func TestParseValidQueryGroupBySameTagkExpandTrue(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host2=*, host2=host3})|merge(sum, downsample(1m, min, none, query(testParseExpression2, null, 5m)))`)

	query := fmt.Sprintf("expand=true&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric2, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagHost2, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, tagHost3, response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseValidQueryGroupBySameTagkExpandFalse(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host2=*, host2=host3})|merge(sum, downsample(1m, min, none, query(testParseExpression2, null, 5m)))`)

	filtersGB := []tools.TSDBfilter{
		{
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "*",
			GroupBy: true,
		}, {
			Ftype:   "wildcard",
			Tagk:    "host2",
			Filter:  "host3",
			GroupBy: true,
		},
	}

	query := fmt.Sprintf("expand=false&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric2, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 2, len(response[0].Queries[0].Filters))
	assert.Contains(t, filtersGB, response[0].Queries[0].Filters[0])
	assert.Contains(t, filtersGB, response[0].Queries[0].Filters[1])

}

func TestParseValidQuerySameTagkOnGroupByAndTags(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host2=*})|merge(sum, downsample(1m, min, none, query(testParseExpression2, {host2=host3}, 5m)))`)

	query := fmt.Sprintf("expand=true&exp=%s&ksid=%s", expression, ksMycenae)
	status, response := parseExp(t, query)

	assert.Equal(t, 200, status)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, 1, len(response[0].Queries))
	assert.Equal(t, "5m", response[0].Relative)
	assert.Equal(t, "sum", response[0].Queries[0].Aggregator)
	assert.Equal(t, "1m-min-none", response[0].Queries[0].Downsample)
	assert.Equal(t, metric2, response[0].Queries[0].Metric)
	assert.Equal(t, 0, len(response[0].Queries[0].Tags))
	assert.Equal(t, false, response[0].Queries[0].RateOptions.Counter)
	assert.Equal(t, (*int64)(nil), response[0].Queries[0].RateOptions.CounterMax)
	assert.Equal(t, int64(0), response[0].Queries[0].RateOptions.ResetValue)
	assert.Equal(t, 2, len(response[0].Queries[0].Order))
	assert.Equal(t, "downsample", response[0].Queries[0].Order[0])
	assert.Equal(t, "aggregation", response[0].Queries[0].Order[1])
	assert.Equal(t, 1, len(response[0].Queries[0].Filters))
	assert.Equal(t, "wildcard", response[0].Queries[0].Filters[0].Ftype)
	assert.Equal(t, tagHost2, response[0].Queries[0].Filters[0].Tagk)
	assert.Equal(t, tagHost3, response[0].Queries[0].Filters[0].Filter)
	assert.Equal(t, false, response[0].Queries[0].Filters[0].GroupBy)

}

func TestParseInvalidQuery(t *testing.T) {

	cases := map[string]struct {
		exp string
		err string
		msg string
	}{
		"InvalidTagKey": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {*=test}, 5m)))`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"InvalidMetric": {
			`merge(sum, downsample(1m, min, none, query(*, {host=test}, 5m)))`,
			"Invalid characters in field metric: *",
			"Invalid characters in field metric: *",
		},
		"DownsampleWithoutMerge": {
			`downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeInvalidExpression": {
			`merge(x, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeEmptyExpression": {
			`merge(downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"merge expects 2 parameters but found 1: [downsample(1m,min,none,query(os.cpu,{app=nonexistent},5m))]",
			"merge needs 2 parameters: merge operation and a function",
		},
		"MergeNullExpression": {
			`merge(null, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeInvalidFunction": {
			`merge(sum, x(1m, min, query(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown function x",
			"unkown function x",
		},
		"NullFunction": {
			`merge(sum, null(1m, min, query(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown function null",
			"unkown function null",
		},
		"EmptyFunction": {
			`merge(sum, (1m, min, query(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown function ",
			"unkown function ",
		},
		"DuplicateRate": {
			`rate(false, null, 0, merge(sum, rate(false, null, 0, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))))`,
			"You can use only one rate function per expression",
			"You can use only one rate function per expression",
		},
		"DuplicateMerge": {
			`merge(sum, merge(min, query(os.cpu, {app=nonexistent}, 5m)))`,
			"You can use only one merge function per expression",
			"You can use only one merge function per expression",
		},
		"DuplicateDownsample": {
			`downsample(1m, sum, none, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"You can use only one downsample function per expression",
			"You can use only one downsample function per expression",
		},
		"DownsampleInvalidPeriod": {
			`merge(sum, downsample(0m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"interval needs to be bigger than 0",
			"interval needs to be bigger than 0",
		},
		"DownsampleInvalidPeriod2": {
			`merge(sum, downsample(1, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"DownsampleNullPeriod": {
			`merge(sum, downsample(null, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"Invalid unit",
			"Invalid unit",
		},
		"DownsampleEmptyPeriod": {
			`merge(sum, downsample(x, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"DownsampleInvalidExpression": {
			`merge(sum, downsample(1m, x, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"DownsampleNullExpression": {
			`merge(sum, downsample(1m, null, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"DownsampleEmptyExpression": {
			`merge(sum, downsample(1m, min, none, x(os.cpu, {app=nonexistent}, 5m)))`,
			"unkown function x",
			"unkown function x",
		},
		"NoQuery": {
			`merge(sum, downsample(1m, min, none, (os.cpu, {app=nonexistent}, 5m)))`,
			"unkown function ",
			"unkown function ",
		},
		"EmptyMetric": {
			`merge(sum, downsample(1m, min, none, query({app=nonexistent}, 5m)))`,
			"query expects 3 parameters but found 2: [{app=nonexistent} 5m]",
			"query needs 3 parameters: metric, map or null and a time interval",
		},
		"EmptyTag": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, , 5m)))`,
			"empty map",
			"empty map",
		},
		"EmptyTagValue": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {app=}, 5m)))`,
			"map value cannot be empty",
			"map value cannot be empty",
		},
		"EmptyTagKey": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {=test}, 5m)))`,
			"map key cannot be empty",
			"map key cannot be empty",
		},
		"EmptyRelative": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent})))`,
			"query expects 3 parameters but found 2: [os.cpu {app=nonexistent}]",
			"query needs 3 parameters: metric, map or null and a time interval",
		},
		"InvalidRelative": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"InvalidRelative2": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"NullRelative": {
			`merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, null)))`,
			"Invalid unit",
			"Invalid unit",
		},
		"DownsampleExtraParamsRelative": {
			`merge(sum, downsample(1m ,1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"downsample expects 4 parameters but found 5: [1m 1m min none query(os.cpu,{app=nonexistent},5m)]",
			"downsample needs 4 parameters: downsample operation, downsample period, fill option and a function",
		},
		"MergeExtraParamsRelative": {
			`merge(sum, sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m)))`,
			"merge expects 2 parameters but found 3: [sum sum downsample(1m,min,none,query(os.cpu,{app=nonexistent},5m))]",
			"merge needs 2 parameters: merge operation and a function",
		},
		"Expression": {
			`x`,
			"unkown function x",
			"unkown function x",
		},
		"InvalidGroupBy": {
			`x|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"unkown function x|rate",
			"unkown function x|rate",
		},
		"GroupByRegexTagkAndTagv": {
			`groupBy({*=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"GroupByRegexTagk": {
			`groupBy({*=host})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"GroupByNoTags": {
			`groupBy({})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"bad map format",
			"bad map format",
		},
		"EmptyGroupBy": {
			`groupBy()|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"empty map",
			"empty map",
		},
		"InvalidRate": {
			`rate(, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseBool: parsing \"\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"NullRate": {
			`rate(null, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseBool: parsing \"null\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"RateEmptyCounter": {
			`rate(, null , 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseBool: parsing \"\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"Counter": {
			`rate("x", null , 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseBool: parsing \"\\\"x\\\"\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"RateEmptyCountermax": {
			`rate(true, , 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseInt: parsing \"\": invalid syntax",
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
		},
		"RateInvalidCountermax": {
			`rate(true, x, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseInt: parsing \"x\": invalid syntax",
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
		},
		"RateNegativeCountermax": {
			`rate(true, -1, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"counter max needs to be a positive integer",
			"counter max needs to be a positive integer",
		},
		"RateEmptyResetvalue": {
			`rate(true, null, , merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseInt: parsing \"\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
		"RateNullResetvalue": {
			`rate(true, null, null, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseInt: parsing \"null\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
		"RateInvalidResetvalue": {
			`rate(true, null, x, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`,
			"strconv.ParseInt: parsing \"x\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
		"FilterValueOperator": {
			`merge(max,downsample(1m,sum,none,filter(x5,query(os.cpu,{app=nonexistent},5m))))`,
			"invalid filter value x5",
			"invalid filter value x5",
		},
		"FilterValueEmptyOperator": {
			`merge(max,downsample(1m,sum,none,filter(5,query(os.cpu,{app=nonexistent},5m))))`,
			"invalid filter value 5",
			"invalid filter value 5",
		},
		"FilterValueEmptyNumber": {
			`merge(max,downsample(1m,sum,none,filter(>,query(os.cpu,{app=nonexistent},5m))))`,
			"invalid filter value >",
			"invalid filter value >",
		},
		"ParseEmptyQueryExpression": {
			``,
			"no expression found",
			"no expression found",
		},
	}

	for test, data := range cases {

		path := fmt.Sprintf("expression/parse?exp=%s&ksid=%s", url.QueryEscape(data.exp), ksMycenae)
		parseAssertInvalidExp(t, path, test, data.err, data.msg)
	}

}

func TestParseQueryExpressionNotSent(t *testing.T) {

	path := fmt.Sprintf("expression/parse?ksid=%s", ksMycenae)
	respErrMsg := "no expression found"

	parseAssertInvalidExp(t, path, "", respErrMsg, respErrMsg)
}

func TestParseInvalidQueryGroupByKeyspaceNotSent(t *testing.T) {

	exp := `groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=test}, 5m))))`
	path := fmt.Sprintf("expression/parse?exp=%v&expand=true", url.QueryEscape(exp))
	respErrMsg := "When expand true, ksid can not be empty"

	parseAssertInvalidExp(t, path, "", respErrMsg, respErrMsg)
}

func TestParseInvalidQueryGroupByKeyspaceNotFound(t *testing.T) {

	exp := `groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=nonexistent}, 5m))))`
	path := fmt.Sprintf("expression/parse?exp=%v&ksid=fake&expand=true", url.QueryEscape(exp))
	respErrMsg := "ksid not found"

	parseAssertInvalidExp(t, path, "", respErrMsg, respErrMsg)
}
