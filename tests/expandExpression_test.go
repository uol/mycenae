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

func sendPointsExpandExp(keySet string) {

	fmt.Println("Setting up expandExpression_test.go tests...")

	metric1 := "testExpandExpression"
	metric2 := "testExpandExpression2"
	tagApp := "app"
	tagApp1 := "app1"
	tagHost := "host"
	tagHost1 := "host1"
	tagHost2 := "host2"
	tagHost3 := "host3"
	tagService := "service"
	tagService1 := "service1"
	tagService2 := "service2"

	points := `[
	  {
		"value": 36.5,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keySet + `",
		  "` + tagHost + `":"` + tagHost1 + `"
		}
	  },
	  {
		"value": 54.5,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost + `":"` + tagHost2 + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 5.4,
		"metric": "` + metric1 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost + `":"` + tagHost3 + `"
		},
		"timestamp": 1444166564000
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagApp + `":"` + tagApp1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagService + `":"` + tagService1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagService + `":"` + tagService1 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagService + `":"` + tagService1 + `"
		}
	  },
	  {
		"value": 1.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost1 + `",
	      "` + tagService + `":"` + tagService2 + `"
		},
		"timestamp": 1448315804000
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost2 + `",
	      "` + tagService + `":"` + tagService2 + `"
		}
	  },
	  {
		"value": 50.1,
		"metric": "` + metric2 + `",
		"tags": {
		  "ksid": "` + keySet + `",
	      "` + tagHost2 + `":"` + tagHost3 + `",
	      "` + tagService + `":"` + tagService2 + `"
		}
	  }
	]`

	code, resp, _ := mycenaeTools.HTTP.POST("api/put", []byte(points))
	if code != http.StatusNoContent {
		log.Fatal("Error sending points! - expandExpression_test.go, Code: ", code, " ", string(resp))
	}
}

func TestExpandValidQuery(t *testing.T) {

	cases := map[string]struct {
		expr     string
		response []string
	}{
		"MetricAndTagsWithSpecialChar": {
			"merge(sum,downsample(30s,min,none,query(os-%&#;_/.cpu,{ap-%&#;_/.p=tes-%&#;_/.t},5m)))",
			[]string{"merge(sum,downsample(30s,min,none,query(os-%&#;_/.cpu,{ap-%&#;_/.p=tes-%&#;_/.t},5m)))"},
		},
		"DownsampleSec": {
			"merge(sum,downsample(30s,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(30s,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleHours": {
			"merge(sum,downsample(2h,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(2h,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleDays": {
			"merge(sum,downsample(1d,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1d,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleWeeks": {
			"merge(sum,downsample(3w,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(3w,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleMonths": {
			"merge(sum,downsample(2n,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(2n,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleYear": {
			"merge(sum,downsample(1y,min,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1y,min,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleMax": {
			"merge(sum,downsample(1m,max,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1m,max,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleAvg": {
			"merge(sum,downsample(1m,avg,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1m,avg,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleSum": {
			"merge(sum,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleCount": {
			"merge(sum,downsample(1m,count,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(sum,downsample(1m,count,none,query(os.cpu,{app=test},5m)))"},
		},
		"DownsampleFillNull": {
			"rate(true, null, 0, merge(sum, downsample(1m, min, null,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,null,query(testExpandExpression2,{app=app1},5m))))"},
		},
		"DownsampleFillNan": {
			"rate(true, null, 0, merge(sum, downsample(1m, min, nan,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,nan,query(testExpandExpression2,{app=app1},5m))))"},
		},
		"DownsampleFillZero": {
			"rate(true, null, 0, merge(sum, downsample(1m, min, zero,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,zero,query(testExpandExpression2,{app=app1},5m))))"},
		},
		"MergeMax": {
			"merge(max,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(max,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))"},
		},
		"MergeAvg": {
			"merge(avg,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(avg,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))"},
		},
		"MergeMin": {
			"merge(min,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(min,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))"},
		},
		"MergeCount": {
			"merge(count,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))",
			[]string{"merge(count,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))"},
		},
		"FilterValuesGreaterThan": {
			"merge(max,downsample(1m,sum,none,filter(>5,query(os.cpu,{app=test},5m))))",
			[]string{"merge(max,downsample(1m,sum,none,filter(>5,query(os.cpu,{app=test},5m))))"},
		},
		"FilterValuesGreaterThanEqualTo": {
			"merge(max,downsample(1m,sum,none,filter(>=5,query(os.cpu,{app=test},5m))))",
			[]string{"merge(max,downsample(1m,sum,none,filter(>=5,query(os.cpu,{app=test},5m))))"},
		},
		"FilterValuesLessThan": {
			"merge(max,downsample(1m,sum,none,filter(<5,query(os.cpu,{app=test},5m))))",
			[]string{"merge(max,downsample(1m,sum,none,filter(<5,query(os.cpu,{app=test},5m))))"},
		},
		"FilterValuesLessThanEqualTo": {
			"merge(max,downsample(1m,sum,none,filter(<=5,query(os.cpu,{app=test},5m))))",
			[]string{"merge(max,downsample(1m,sum,none,filter(<=5,query(os.cpu,{app=test},5m))))"},
		},
		"FilterValuesEqualTo": {
			"merge(max,downsample(1m,sum,none,filter(==5,query(os.cpu,{app=test},5m))))",
			[]string{"merge(max,downsample(1m,sum,none,filter(==5,query(os.cpu,{app=test},5m))))"},
		},
		"RelativeSec": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},30s)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},30s)))"},
		},
		"RelativeHour": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2h)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2h)))"},
		},
		"RelativeDay": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1d)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1d)))"},
		},
		"RelativeWeek": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},3w)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},3w)))"},
		},
		"RelativeMonth": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2n)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2n)))"},
		},
		"RelativeYear": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1y)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1y)))"},
		},
		"MoreThanOneTag": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test,host=host1,cpu=1},5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test,cpu=1,host=host1},5m)))"},
		},
		"NoTags": {
			"merge(sum,downsample(1m,min,none,query(os.cpu,null,5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(os.cpu,null,5m)))"},
		},
		"OrderMergeDownsampleRate": {
			"rate(false,null,0,downsample(1m,min,none,merge(sum,query(os.cpu,{app=test},5m))))",
			[]string{"rate(false,null,0,downsample(1m,min,none,merge(sum,query(os.cpu,{app=test},5m))))"},
		},
		"OrderMergeRateDownsampleFilterValue": {
			"filter(>5, downsample(1m, min, none, rate(false, null, 0, merge(min, query(testExpandExpression2, {app=app1}, 5m)))))",
			[]string{"filter(>5,downsample(1m,min,none,rate(false,null,0,merge(min,query(testExpandExpression2,{app=app1},5m)))))"},
		},
		"OrderFilterValueRateMergeDownsample": {
			"downsample(1m, min, none, merge(min, rate(false, null, 0, filter(==5, query(testExpandExpression2, {app=app1}, 5m)))))",
			[]string{"downsample(1m,min,none,merge(min,rate(false,null,0,filter(==5,query(testExpandExpression2,{app=app1},5m)))))"},
		},
		"OrderRateFilterValueDownsampleMerge": {
			"merge(count, downsample(1m, min, null, filter(<= 5, rate(true, 1, 2, query(testExpandExpression2, {app=app1}, 5m)))))",
			[]string{"merge(count,downsample(1m,min,null,filter(<=5,rate(true,1,2,query(testExpandExpression2,{app=app1},5m)))))"},
		},
		"OrderMergeRateDownsample": {
			"downsample(1m, min, none, rate(false, null, 0, merge(min, query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"downsample(1m,min,none,rate(false,null,0,merge(min,query(testExpandExpression2,{app=app1},5m))))"},
		},
		"MergeWithoutDownsample": {
			"merge(sum,query(os.cpu,{app=test},5m))",
			[]string{"merge(sum,query(os.cpu,{app=test},5m))"},
		},
		"GroupbyExpandMatchNoTags": {
			"groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression, null, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression,{host=host1},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression,{host=host2},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression,{host=host3},5m))))"},
		},
		"GroupbyExpandMatchWithTags": {
			"groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"GroupbyWithoutRate": {
			"groupBy({host2=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m)))"},
		},
		"RateTrueNoCounterMaxNoResetValue": {
			"groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(true,null,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"RateTrueAndCounterMaxNoResetValue": {
			"groupBy({host2=*})|rate(true, 10, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,10,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(true,10,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(true,10,0,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"RateTrueNoCounterMaxAndResetValue": {
			"groupBy({host2=*})|rate(true, null, 10, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,null,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(true,null,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(true,null,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"RateTrueCounterMaxAndResetValue": {
			"groupBy({host2=*})|rate(true, 10, 10, merge(sum, downsample(1m, min,none, query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(true,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(true,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(true,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"RateFalseCounterMaxAndResetValue": {
			"groupBy({host2=*})|rate(false, 10, 10, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))",
			[]string{"rate(false,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host1},5m))))",
				"rate(false,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host2},5m))))",
				"rate(false,10,10,merge(sum,downsample(1m,min,none,query(testExpandExpression2,{app=app1,host2=host3},5m))))"},
		},
		"GroupbyTwoTags": {
			"groupBy({host2=*, service=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, null, 5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host1,service=service1},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host1,service=service2},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host2,service=service1},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host2,service=service2},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host3,service=service1},5m)))",
				"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host3,service=service2},5m)))"},
		}, "GroupBySameTagk": {
			"groupBy({host2=*, host2=host3})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, null, 5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host3},5m)))"},
		},
		"SameTagkOnGroupByAndTags": {
			"groupBy({host2=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, {host2=host3}, 5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(testExpandExpression2,{host2=host3},5m)))"},
		},
		"SameTagkOrder": {
			"merge(sum, downsample(1m,min,none,query(testParseExpression2,{host2=host1,host2=host3,host2=host2,app=app2,app=app3,app=app1}, 5m)))",
			[]string{"merge(sum,downsample(1m,min,none,query(testParseExpression2,{app=app1,app=app2,app=app3,host2=host1,host2=host2,host2=host3},5m)))"},
		},
	}

	for test, data := range cases {

		apiUrl := fmt.Sprintf("keysets/%s/expression/expand?exp=%s", ksMycenae, url.QueryEscape(data.expr))
		statusCode, resp, err := mycenaeTools.HTTP.GET(apiUrl)
		if err != nil {
			t.Error(test, err)
			t.SkipNow()
		}

		assert.Equal(t, 200, statusCode, test)

		response := []string{}

		err = json.Unmarshal(resp, &response)
		if err != nil {
			t.Error(test, err, "URL:", apiUrl)
			t.SkipNow()
		}

		assert.Equal(t, len(data.response), len(response), test)

		for i := 0; i < len(response); i++ {

			assert.Equal(t, data.response[i], response[i], test)
		}
	}

}

func TestExpandInvalidQuerySameErrorMessage(t *testing.T) {

	cases := map[string]struct {
		expr      string
		errorMess string
	}{
		"WithoutMerge": {
			"downsample(1m, min, none,query(os.cpu, {app=test}, 5m))",
			"unkown aggregation value",
		},
		"MergeInvalidExpression": {
			"merge(x, downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"unkown aggregation value",
		},
		"MergeNullExpression": {
			"merge(null, downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"unkown aggregation value",
		},
		"MergeInvalidFunction": {
			"merge(sum, x(1m, min, query(os.cpu, {app=test}, 5m)))",
			"unkown function x",
		},
		"NullFunction": {
			"merge(sum, null(1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"unkown function null",
		},
		"EmptyFunction": {
			"merge(sum, (1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"unkown function ",
		},
		"DuplicateMerge": {
			"merge(sum, merge(min, query(os.cpu, {app=test}, 5m)))",
			"You can use only one merge function per expression",
		},
		"DuplicateDownsample": {
			"downsample(1m, sum, none,downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"You can use only one downsample function per expression",
		},
		"DownsampleInvalidInterval": {
			"merge(sum, downsample(0m, min, none,query(os.cpu, {app=test}, 5m)))",
			"interval needs to be bigger than 0",
		},
		"DownsampleInvalidTimeInterval": {
			"merge(sum, downsample(1, min,none, query(os.cpu, {app=test}, 5m)))",
			"Invalid time interval",
		},
		"DownsampleNullPeriod": {
			"merge(sum, downsample(null, min, none,query(os.cpu, {app=test}, 5m)))",
			"Invalid unit",
		},
		"DownsampleEmptyPeriod": {
			"merge(sum, downsample(x, min,none, query(os.cpu, {app=test}, 5m)))",
			"Invalid time interval",
		},
		"DownsampleInvalidExpression": {
			"merge(sum, downsample(1m, x, none,query(os.cpu, {app=test}, 5m)))",
			"Invalid downsample",
		},
		"DownsampleNullExpression": {
			"merge(sum, downsample(1m, null, none,query(os.cpu, {app=test}, 5m)))",
			"Invalid downsample",
		},
		"DownsampleEmptyExpression": {
			"merge(sum, downsample(1m, min,none, x(os.cpu, {app=test}, 5m)))",
			"unkown function x",
		},
		"NoQuery": {
			"merge(sum, downsample(1m, min,none, (os.cpu, {app=test}, 5m)))",
			"unkown function ",
		},
		"RegexMetric": {
			"merge(sum, downsample(1m, min,none, query(*, {app=test}, 5m)))",
			"Invalid characters in field metric: *",
		},
		"EmptyTag": {
			"merge(sum, downsample(1m, min,none, query(os.cpu, , 5m)))",
			"empty map",
		},
		"EmptyTagValue": {
			"merge(sum, downsample(1m, min,none,query(os.cpu, {app=}, 5m)))",
			"map value cannot be empty",
		},
		"EmptyTagKey": {
			"merge(sum, downsample(1m, min, none,query(os.cpu, {=test}, 5m)))",
			"map key cannot be empty",
		},
		"InvalidRelative": {
			"merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5)))",
			"Invalid time interval",
		},
		"InvalidRelative2": {
			"merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, m)))",
			"Invalid time interval",
		},
		"NullRelative": {
			"merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, null)))",
			"Invalid unit",
		},
		"InvalidExpression": {
			"x",
			"unkown function x",
		},
		"EmptyQueryExpression": {
			"",
			"no expression found",
		},
		"InvalidGroupBy": {
			"x|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))",
			"unkown function x|rate",
		},
		"GroupByRegexTagkAndTagv": {
			"groupBy({*=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))",
			"Invalid characters in field tagk: *",
		},
		"GroupByRegexTagk": {
			"groupBy({*=host})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"Invalid characters in field tagk: *",
		},
		"GroupByNoTags": {
			"groupBy({})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))",
			"bad map format",
		},
		"EmptyGroupBy": {
			"groupBy()|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"empty map",
		},
		"RateNegativeCountermax": {
			"rate(true, -1, 0, merge(sum, downsample(1m, min, none, query(os.cpu, {app=test}, 5m))))",
			"counter max needs to be a positive integer",
		},
		"InvalidFilterValueOperator": {
			"merge(max,downsample(1m,sum,none,filter(x5,query(os.cpu,{app=test},5m))))",
			"invalid filter value x5",
		},
		"FilterValueEmptyOperator": {
			"merge(max,downsample(1m,sum,none,filter(5,query(os.cpu,{app=test},5m))))",
			"invalid filter value 5",
		},
		"FilterValueEmptyNumber": {
			"merge(max,downsample(1m,sum,none,filter(>,query(os.cpu,{app=test},5m))))",
			"invalid filter value >",
		},
	}

	for test, data := range cases {

		expandAssertInvalidExp(t, test, url.QueryEscape(data.expr), data.errorMess, data.errorMess)
	}
}

func TestExpandInvalidQueryDifferentErrorMessage(t *testing.T) {

	cases := map[string]struct {
		expr    string
		error   string
		message string
	}{
		"MergeEmptyExpression": {
			"merge(downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))",
			"merge expects 2 parameters but found 1: [downsample(1m,min,none,query(os.cpu,{app=test},5m))]",
			"merge needs 2 parameters: merge operation and a function",
		},
		"EmptyMetric": {
			"merge(sum, downsample(1m, min,none,query({app=test}, 5m)))",
			"query expects 3 parameters but found 2: [{app=test} 5m]",
			"query needs 3 parameters: metric, map or null and a time interval",
		},
		"EmptyRelative": {
			"merge(sum, downsample(1m, min,none, query(os.cpu, {app=test})))",
			"query expects 3 parameters but found 2: [os.cpu {app=test}]",
			"query needs 3 parameters: metric, map or null and a time interval",
		},
		"DownsampleExtraParamsRelative": {
			"merge(sum, downsample(1m ,1m, min,none,query(os.cpu, {app=test}, 5m)))",
			"downsample expects 4 parameters but found 5: [1m 1m min none query(os.cpu,{app=test},5m)]",
			"downsample needs 4 parameters: downsample operation, downsample period, fill option and a function",
		},
		"MergeExtraParamsRelative": {
			"merge(sum, sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m)))",
			"merge expects 2 parameters but found 3: [sum sum downsample(1m,min,none,query(os.cpu,{app=test},5m))]",
			"merge needs 2 parameters: merge operation and a function",
		},
		"InvalidRate": {
			"rate(, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseBool: parsing \"\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"NullRate": {
			"rate(null, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseBool: parsing \"null\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"RateEmptyCounter": {
			"rate(, null , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseBool: parsing \"\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"InvalidCounter": {
			"rate(x, null , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseBool: parsing \"x\": invalid syntax",
			"rate counter, the 1st parameter, needs to be a boolean",
		},
		"RateEmptyCountermax": {
			"rate(true, , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseInt: parsing \"\": invalid syntax",
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
		},
		"RateInvalidCountermax": {
			"rate(true, x, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseInt: parsing \"x\": invalid syntax",
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
		},
		"RateEmptyResetvalue": {
			"rate(true, null, , merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseInt: parsing \"\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
		"RateNullResetvalue": {
			"rate(true, null, null, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseInt: parsing \"null\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
		"RateInvalidResetvalue": {
			"rate(true, null, x, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))",
			"strconv.ParseInt: parsing \"x\": invalid syntax",
			"rate resetValue, the 3rd parameter, needs to be an integer",
		},
	}

	for test, data := range cases {

		expandAssertInvalidExp(t, test, url.QueryEscape(data.expr), data.error, data.message)
	}
}

func TestExpandValidQueryGroupbyExpandDontMatch(t *testing.T) {

	query := `groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(testExpandExpression, {app=test2}, 5m))))`

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keysets/%s/expression/expand?exp=%s`, ksMycenae, url.QueryEscape(query)))

	assert.Equal(t, 204, statusCode)
	assert.Equal(t, []byte{}, resp)
}

func TestExpandQueryExpressionNotSent(t *testing.T) {

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keysets/%s/expression/expand`, ksMycenae))

	response := tools.Error{}

	err := json.Unmarshal(resp, &response)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "no expression found", response.Error)
	assert.Equal(t, "no expression found", response.Message)
}

func TestExpandInvalidQueryGroupByKeyspaceNotFound(t *testing.T) {

	expression := url.QueryEscape(`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`)

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keyspaces/aaa/expression/expand?exp=%s`, expression))

	assert.Equal(t, 404, statusCode)
	assert.Equal(t, 0, len(resp))
}

func TestExpandInvalidQueryGroupByKeyspaceNotSent(t *testing.T) {

	expression := url.QueryEscape(`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`)

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keyspaces/expression/expand?exp=%s`, expression))

	assert.Equal(t, 404, statusCode)
	assert.Equal(t, 0, len(resp))
}

func expandAssertInvalidExp(t *testing.T, test, urlQuery, respErr, respMessage string) {

	status, resp, err := mycenaeTools.HTTP.GET(fmt.Sprintf("keysets/%s/expression/expand?exp=%s", ksMycenae, urlQuery))
	if err != nil {
		t.Error(test, err)
		t.SkipNow()
	}

	response := tools.Error{}

	err = json.Unmarshal(resp, &response)
	if err != nil {
		t.Error(test, err)
		t.SkipNow()
	}

	assert.Equal(t, 400, status, test)
	assert.Equal(t, respErr, response.Error, test)
	assert.Equal(t, respMessage, response.Message, test)
}
