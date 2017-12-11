package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func sendPointsTsdbAggAndSugAndLookup(keyspace string) {

	fmt.Println("Setting up tsdbAggAndSugAndLookup_test.go tests...")

	point := `[
  {
    "value": 36.5,
    "metric": "os.cpu",
    "tags": {
      "ksid": "` + keyspace + `",
      "host": "a1-testTsdbMeta"
    }
  },
  {
    "value": 54.5,
    "metric": "os.cpu-_/.%&#;Test",
    "tags": {
      "ksid": "` + keyspace + `",
      "host": "a2-testTsdb-_/.%&#;Meta"
    },
    "timestamp": 1444166564000
  },
  {
    "value": 5.4,
    "metric": "execution.time",
    "tags": {
      "ksid": "` + keyspace + `",
      "host-_/.%&#;Name": "a1-testTsdbMeta"
    },
    "timestamp": 1444166564000
  },
  {
    "value": 1.1,
    "metric": "os.cpu",
    "tags": {
      "ksid": "` + keyspace + `",
      "host": "a2-testTsdb-_/.%&#;Meta"
    },
    "timestamp": 1448315804000
  },
  {
    "value": 50.1,
    "metric": "os.cpu",
    "tags": {
      "ksid": "` + keyspace + `",
      "host": "a1-testTsdbMeta"
    }
  },
  {
    "value": 1,
    "metric": "os.cpu",
    "tags": {
      "ksid": "` + keyspace + `",
      "host": "a1-testTsdbMeta",
      "cpu": "1"
    }
  }
]`

	code, resp, err := mycenaeTools.HTTP.POST("api/put", []byte(point))
	if err != nil || code != 204 {
		log.Fatal("send points", code, string(resp), err)
	}
}

func TestTsdb(t *testing.T) {

	cases := map[string]struct {
		url      string
		expected []string
		size     int
	}{
		"Aggregator": {
			fmt.Sprintf("keyspaces/%s/api/aggregators", ksMycenaeTsdb),
			[]string{"avg", "count", "min", "max", "sum"},
			5,
		},
		"SuggestMetrics": {
			fmt.Sprintf("keyspaces/%s/api/suggest?type=metrics", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpu-_/.%&#;Test"},
			3,
		},
		"SuggestTagk": {
			fmt.Sprintf("keyspaces/%s/api/suggest?type=tagk", ksMycenaeTsdb),
			[]string{"cpu", "host", "host-_/.%&#;Name"},
			3,
		},
		"SuggestTagv": {
			fmt.Sprintf("keyspaces/%s/api/suggest?type=tagv", ksMycenaeTsdb),
			[]string{"1", "a1-testTsdbMeta", "a2-testTsdb-_/.%&#;Meta"},
			3,
		},
		"SuggestMetricsMax": {
			fmt.Sprintf("keyspaces/%s/api/suggest?type=metrics&max=1", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpu-_/.%&#;Test"},
			1,
		},
		"SuggestOverMax": {
			fmt.Sprintf("keyspaces/%s/api/suggest?type=metrics&max=4", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpu-_/.%&#;Test"},
			3,
		},
	}

	for test, data := range cases {

		code, response, err := mycenaeTools.HTTP.GET(data.url)
		if err != nil {
			t.Error(err)
			t.SkipNow()
		}

		assert.Equal(t, 200, code, test)

		respList := []string{}

		err = json.Unmarshal(response, &respList)
		if err != nil {
			t.Error(err)
			t.SkipNow()
		}

		assert.Equal(t, data.size, len(respList), "the total records are different than expected", test)

		sort.Strings(data.expected)
		sort.Strings(respList)

		if test == "SuggestMetricsMax" {
			assert.Contains(t, data.expected, respList[0], "the metric is different than expected", test)
		} else {
			assert.Equal(t, data.expected, respList, fmt.Sprintf("%s: FOUND: %v, EXPECTED: %v", test, respList, data.expected))
		}

	}

}

func TestTsdbLookupMetricFullNameMoreThanOneResult(t *testing.T) {

	var (
		lookupExpected1 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}},{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}},{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}}],"startIndex":0,"totalResults":3}`
		lookupExpected2 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}},{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}},{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}}],"startIndex":0,"totalResults":3}`
		lookupExpected3 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}},{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}},{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}}],"startIndex":0,"totalResults":3}`
		lookupExpected4 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}},{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}},{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}}],"startIndex":0,"totalResults":3}`
		lookupExpected5 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}},{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}},{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}}],"startIndex":0,"totalResults":3}`
		lookupExpected6 = `{"type":"LOOKUP","metric":"os.cpu","tags":[],"limit":0,"time":0,"results":[{"tsuid":"3052027985","metric":"os.cpu","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}},{"tsuid":"1308439016","metric":"os.cpu","tags":{"host":"a1-testTsdbMeta"}},{"tsuid":"2553471299","metric":"os.cpu","tags":{"cpu":"1","host":"a1-testTsdbMeta"}}],"startIndex":0,"totalResults":3}`
	)

	code, response, err := mycenaeTools.HTTP.GET(fmt.Sprintf("keyspaces/%s/api/search/lookup?m=os.cpu", ksMycenaeTsdb))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Condition(
		t,
		func() bool {
			return (string(response) == lookupExpected1) || (string(response) == lookupExpected2) || (string(response) == lookupExpected3) || (string(response) == lookupExpected4) || (string(response) == lookupExpected5) || (string(response) == lookupExpected6)
		},
		fmt.Sprintf("FOUND: %v, EXPECTED: %v or %v or %v or %v or %v or %v", string(response), lookupExpected1, lookupExpected2, lookupExpected3, lookupExpected4, lookupExpected5, lookupExpected6),
	)

}

func TestTsdbLookupMetricFullNameOnlyOneResult(t *testing.T) {

	lookupExpected := `{"type":"LOOKUP","metric":"os.cpu-_/.%\u0026#;Test","tags":[],"limit":0,"time":0,"results":[{"tsuid":"2323443649","metric":"os.cpu-_/.%\u0026#;Test","tags":{"host":"a2-testTsdb-_/.%\u0026#;Meta"}}],"startIndex":0,"totalResults":1}`

	metricEscape := url.QueryEscape("os.cpu-_/.%\u0026#;Test")

	code, response, err := mycenaeTools.HTTP.GET(fmt.Sprintf("keyspaces/%s/api/search/lookup?m=%s", ksMycenaeTsdb, metricEscape))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.Condition(
		t,
		func() bool { return string(response) == lookupExpected },
		fmt.Sprintf("FOUND: %v, EXPECTED: %v", string(response), lookupExpected),
	)
}

//
//func TestTsdbLookupInvalidMetric(t *testing.T) {
//
//    lookupList := LookupError{}
//
//    code := tsdbLookupTools.HTTP.GETjson(fmt.Sprintf("keyspaces/"+ keyspacetsdbLookup +"/api/search/lookup?m=xxx"), &lookupList)
//
//    assert.Equal(t, 400, code)
//    assert.Equal(t, "no tsuids found", lookupList.Error, "the total records are different than expected")
//}
//
//func TestTsdbLookupNoMetric(t *testing.T) {
//
//    lookupList := LookupError{}
//
//    code := tsdbLookupTools.HTTP.GETjson(fmt.Sprintf("keyspaces/"+ keyspacetsdbLookup +"/api/search/lookup"), &lookupList)
//
//    assert.Equal(t, 400, code)
//    assert.Equal(t, "missing query parameter \"m\"", lookupList.Error, "the total records are different than expected")
//}
