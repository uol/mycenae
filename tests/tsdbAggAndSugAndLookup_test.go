package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"testing"
)

var (
	payloadArray []tools.Payload
)

func sendPointsTsdbAggAndSugAndLookup(keySet string) {

	fmt.Println("Setting up tsdbAggAndSugAndLookup_test.go tests...")

	payloadArray = []tools.Payload{
		tools.CreatePayload(float32(36.5), "os.cpu", map[string]string{"ksid": keySet, "ttl": "1", "host": "a1-testTsdbMeta"}),
		tools.CreatePayloadTS(float32(54.5), "os.cpuTest", map[string]string{"ksid": keySet, "ttl": "1", "host": "a2-testTsdbMeta"}, int64(1444166564000)),
		tools.CreatePayloadTS(float32(5.4), "execution.time", map[string]string{"ksid": keySet, "ttl": "1", "host": "a1-testTsdbMeta"}, int64(1444166564000)),
		tools.CreatePayloadTS(float32(1.1), "os.cpu", map[string]string{"ksid": keySet, "ttl": "1", "host": "a2-testTsdbMeta"}, int64(1448315804000)),
		tools.CreatePayload(float32(50.1), "os.cpu", map[string]string{"ksid": keySet, "ttl": "1", "host": "a1-testTsdbMeta"}),
		tools.CreatePayload(float32(1), "os.cpu", map[string]string{"ksid": keySet, "ttl": "1", "host": "a1-testTsdbMeta", "cpu": "1"}),
	}

	jsonBytes, err := json.Marshal(payloadArray)

	if err != nil {
		panic(err)
	}

	code, resp, err := mycenaeTools.HTTP.POST("api/put", jsonBytes)
	if err != nil || code != http.StatusNoContent {
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
			fmt.Sprintf("keysets/%s/api/aggregators", ksMycenaeTsdb),
			[]string{"avg", "count", "min", "max", "sum"},
			5,
		},
		"SuggestMetrics": {
			fmt.Sprintf("keysets/%s/api/suggest?type=metrics", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpuTest"},
			3,
		},
		"SuggestTagk": {
			fmt.Sprintf("keysets/%s/api/suggest?type=tagk", ksMycenaeTsdb),
			[]string{"cpu", "host", "ksid", "ttl"},
			4,
		},
		"SuggestTagv": {
			fmt.Sprintf("keysets/%s/api/suggest?type=tagv", ksMycenaeTsdb),
			[]string{"1", "a1-testTsdbMeta", "a2-testTsdbMeta", ksMycenaeTsdb},
			4,
		},
		"SuggestMetricsMax": {
			fmt.Sprintf("keysets/%s/api/suggest?type=metrics&max=1", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpuTest"},
			1,
		},
		"SuggestOverMax": {
			fmt.Sprintf("keysets/%s/api/suggest?type=metrics&max=4", ksMycenaeTsdb),
			[]string{"execution.time", "os.cpu", "os.cpuTest"},
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

func isLookupResultObjectEquals(o1, o2 tools.LookupResultObject) bool {

	isEqual := o1.Type == o2.Type &&
		o1.Metric == o2.Metric &&
		o1.Limit == o2.Limit &&
		o1.Time == o2.Time &&
		o1.TotalResults == o2.TotalResults &&
		o1.StartIndex == o2.StartIndex &&
		len(o1.Tags) == len(o2.Tags) &&
		len(o1.Results) == len(o2.Results) &&
		reflect.DeepEqual(o1.Tags, o2.Tags)

	if !isEqual {
		return false
	}

	m := map[string]tools.LookupResult{}

	for _, v := range o1.Results {
		m[v.TSUID] = v
	}

	for _, v := range o2.Results {
		if item, ok := m[v.TSUID]; !ok {
			return false
		} else {
			if !reflect.DeepEqual(v.Tags, item.Tags) || v.Metric != item.Metric {
				return false
			}
		}
	}

	return true
}

func TestTsdbLookupMetricFullNameMoreThanOneResult(t *testing.T) {

	expectedJson := tools.LookupResultObject{
		Type:   "LOOKUP",
		Metric: "os.cpu",
		Tags:   []string{},
		Limit:  0,
		Time:   0,
		Results: []tools.LookupResult{
			tools.LookupResult{
				TSUID:  tools.GetTSUIDFromPayload(&payloadArray[0], true),
				Tags:   payloadArray[0].Tags,
				Metric: payloadArray[0].Metric,
			},
			tools.LookupResult{
				TSUID:  tools.GetTSUIDFromPayload(&payloadArray[3], true),
				Tags:   payloadArray[3].Tags,
				Metric: payloadArray[3].Metric,
			},
			tools.LookupResult{
				TSUID:  tools.GetTSUIDFromPayload(&payloadArray[5], true),
				Tags:   payloadArray[5].Tags,
				Metric: payloadArray[5].Metric,
			},
		},
		StartIndex:   0,
		TotalResults: 3,
	}

	code, response, err := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%s/api/search/lookup?m=os.cpu", ksMycenaeTsdb))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	returnJson := tools.LookupResultObject{}
	err = json.Unmarshal(response, &returnJson)
	assert.False(t, err != nil, "Error unmarshaling lookup result json: "+string(response))

	assert.Equal(t, 200, code)
	assert.True(
		t,
		isLookupResultObjectEquals(returnJson, expectedJson),
		fmt.Sprintf("FOUND: %v, EXPECTED: %v", returnJson, expectedJson),
	)
}

func TestTsdbLookupMetricFullNameOnlyOneResult(t *testing.T) {

	expectedJson := tools.LookupResultObject{
		Type:   "LOOKUP",
		Metric: "os.cpuTest",
		Tags:   []string{},
		Limit:  0,
		Time:   0,
		Results: []tools.LookupResult{
			tools.LookupResult{
				TSUID:  tools.GetTSUIDFromPayload(&payloadArray[1], true),
				Tags:   payloadArray[1].Tags,
				Metric: payloadArray[1].Metric,
			},
		},
		StartIndex:   0,
		TotalResults: 1,
	}

	metricEscape := url.QueryEscape("os.cpuTest")

	code, response, err := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%s/api/search/lookup?m=%s", ksMycenaeTsdb, metricEscape))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	returnJson := tools.LookupResultObject{}
	err = json.Unmarshal(response, &returnJson)

	assert.Equal(t, 200, code)
	assert.True(
		t,
		isLookupResultObjectEquals(returnJson, expectedJson),
		fmt.Sprintf("FOUND: %v, EXPECTED: %v", returnJson, expectedJson),
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
