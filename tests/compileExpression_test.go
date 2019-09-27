package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func TestCompileValidExpression(t *testing.T) {
	cases := map[string]struct {
		payload    string
		expression string
	}{
		"MetricAndTagsWithSpecialChars": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(30s,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleSec": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(30s,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleHours": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "2h-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(2h,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleDays": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1d-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1d,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleWeeks": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "3w-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(3w,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleMonths": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "2n-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(2n,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleYear": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1y-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1y,min,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleMax": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-max-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,max,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleAvg": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-avg-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,avg,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleSum": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleCount": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-count-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,count,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleFillNotSent": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleFillNull": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-null",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,null,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleFillNan": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-nan",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,nan,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleFillZero": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-zero",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,zero,query(os.cpu,{app=test},5m)))\"]",
		},
		"DownsampleFilterGreaterThan": {
			`{
				"relative": "5m",
				"queries": [{
					"filterValue": ">5",
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,filter(\\u003e5,query(os.cpu,{app=test},5m))))\"]",
		},
		"DownsampleFilterGreaterThanEqualTo": {
			`{
				"relative": "5m",
				"queries": [{
					"filterValue": ">=5",
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,filter(\\u003e=5,query(os.cpu,{app=test},5m))))\"]",
		},
		"DownsampleFilterLessThan": {
			`{
				"relative": "5m",
				"queries": [{
					"filterValue": "<5",
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,filter(\\u003c5,query(os.cpu,{app=test},5m))))\"]",
		},
		"DownsampleFilterLessThanEqualTo": {
			`{
				"relative": "5m",
				"queries": [{
					"filterValue": "<=5",
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,sum,none,filter(\\u003c=5,query(os.cpu,{app=test},5m))))\"]",
		},
		"TypeRegexp": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "regexp",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(30s,min,none,query(os.cpu,{app=regexp(test)},5m)))\"]",
		},
		"TypeLiteralOr": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "literal_or",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(30s,min,none,query(os.cpu,{app=or(test)},5m)))\"]",
		},
		"TypeNotLiteralOr": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "not_literal_or",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(30s,min,none,query(os.cpu,{app=notor(test)},5m)))\"]",
		},
		"MergeMax": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "max",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(max,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"MergeAvg": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "avg",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(avg,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"MergeMin": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "min",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(min,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"MergeCount": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "count",
					"downsample": "1m-sum-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(count,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))\"]",
		},
		"RelativeSec": {
			`{
				"relative": "30s",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},30s)))\"]",
		},
		"RelativeHour": {
			`{
				"relative": "2h",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2h)))\"]",
		},
		"RelativeDay": {
			`{
				"relative": "1d",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1d)))\"]",
		},
		"RelativeWeek": {
			`{
				"relative": "3w",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},3w)))\"]",
		},
		"RelativeMonth": {
			`{
				"relative": "2n",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2n)))\"]",
		},
		"RelativeYear": {
			`{
				"relative": "1y",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1y)))\"]",
		},
		"MoreThanOneTag": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					},{
						"type": "wildcard",
						"tagk": "host",
						"filter": "host1",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,{app=test,host=host1},5m)))\"]",
		},
		"NoTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none"
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(os.cpu,null,5m)))\"]",
		},
		"OrderDownsampleMerge": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"order": ["aggregation", "downsample"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"downsample(1m,min,none,merge(sum,query(os.cpu,{app=test},5m)))\"]",
		},
		"OrderMergeDownsampleRate": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"rate": true,
					"downsample": "1m-min-none",
					"rateOptions": {
						"counter": false
					},
					"order": ["aggregation", "downsample", "rate"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"rate(false,null,0,downsample(1m,min,none,merge(sum,query(os.cpu,{app=test},5m))))\"]",
		},
		"OrderMergeRateDownsampleFilterValue": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"filterValue": ">5",
					"rate": true,
					"downsample": "1m-min-none",
					"order": ["aggregation", "rate", "downsample", "filterValue"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"filter(\\u003e5,downsample(1m,min,none,rate(false,null,0,merge(sum,query(os.cpu,{app=test},5m)))))\"]",
		},
		"OrderFilterValueRateMergeDownsample": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"filterValue": "==5",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"order": ["filterValue", "rate", "aggregation", "downsample"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"downsample(1m,min,none,merge(sum,rate(false,null,0,filter(==5,query(os.cpu,{app=test},5m)))))\"]",
		},
		"OrderRateFilterValueDownsampleMerge": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"filterValue": "<=5",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": 1,
						"resetValue": 2
					},
					"downsample": "1m-min-null",
					"order": ["rate", "filterValue", "downsample", "aggregation"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,null,filter(\\u003c=5,rate(true,1,2,query(os.cpu,{app=test},5m)))))\"]",
		},
		"MergeWithoutDownsample": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,query(os.cpu,{app=test},5m))\"]",
		},
		"GroupbyExpandMatchNoTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression",
					"downsample": "1m-min-none",
					"rate": true,
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "*",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({host=*})|rate(false,null,0,merge(sum,downsample(1m,min,none,query(testParseExpression,null,5m))))\"]",
		},
		"GroupbyExpandMatchWithTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"rate": true,
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": true
					},{
						"type": "wildcard",
						"tagk": "app",
						"filter": "app1",
						"groupBy": false
					}]
				}]
			}`,
			"[\"groupBy({host2=*})|rate(false,null,0,merge(sum,downsample(1m,min,none,query(testParseExpression2,{app=app1},5m))))\"]",
		},
		"GroupbWithoutRate": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": true
					},{
						"type": "wildcard",
						"tagk": "app",
						"filter": "app1",
						"groupBy": false
					}]
				}]
			}`,
			"[\"groupBy({host2=*})|merge(sum,downsample(1m,min,none,query(testParseExpression2,{app=app1},5m)))\"]",
		},
		"GroupBySameTagk": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": true
					},{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "host3",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({host2=*,host2=host3})|merge(sum,downsample(1m,min,none,query(testParseExpression2,null,5m)))\"]",
		},
		"GroupByTypeRegexp": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "regexp",
						"tagk": "app",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({app=regexp(test)})|merge(sum,downsample(30s,min,none,query(os.cpu,null,5m)))\"]",
		},
		"GroupByTypeLiteralOr": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "literal_or",
						"tagk": "app",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({app=or(test)})|merge(sum,downsample(30s,min,none,query(os.cpu,null,5m)))\"]",
		},
		"GroupByTypeNotLiteralOr": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "30s-min-none",
					"filters": [{
						"type": "not_literal_or",
						"tagk": "app",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({app=notor(test)})|merge(sum,downsample(30s,min,none,query(os.cpu,null,5m)))\"]",
		},
		"SameTagkOnGroupByAndTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": true
					},{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "host3",
						"groupBy": false
					}]
				}]
			}`,
			"[\"groupBy({host2=*})|merge(sum,downsample(1m,min,none,query(testParseExpression2,{host2=host3},5m)))\"]",
		},
	}

	for test, data := range cases {

		statusCode, resp, err := mycenaeTools.HTTP.POST("expression/compile", []byte(data.payload))
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		assert.Equal(t, 200, statusCode, test)
		assert.Equal(t, data.expression, string(resp), test)
	}

}

func TestCompileValidTwoExpressions(t *testing.T) {
	cases := map[string]struct {
		payload     string
		expression  string
		expression2 string
	}{
		"GroupbyTwoTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": true
					},{
						"type": "wildcard",
						"tagk": "app",
						"filter": "app1",
						"groupBy": true
					}]
				}]
			}`,
			"[\"groupBy({host2=*,app=app1})|merge(sum,downsample(1m,min,none,query(testParseExpression2,null,5m)))\"]",
			"[\"groupBy({app=app1,host2=*})|merge(sum,downsample(1m,min,none,query(testParseExpression2,null,5m)))\"]",
		},
		"SameTagk": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "testParseExpression2",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "*",
						"groupBy": false
					},{
						"type": "wildcard",
						"tagk": "host2",
						"filter": "host3",
						"groupBy": false
					}]
				}]
			}`,
			"[\"merge(sum,downsample(1m,min,none,query(testParseExpression2,{host2=*,host2=host3},5m)))\"]",
			"[\"merge(sum,downsample(1m,min,none,query(testParseExpression2,{host2=host3,host2=*},5m)))\"]",
		},
		"TwoQueries": {
			`{
				"relative": "5m",
				"queries": [{
					"aggregator": "max",
					"metric": "os.cpu",
					"downsample": "10m-count-null",
					"filterValue": ">5",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": 100
					},
					"order": ["rate", "downsample", "aggregation", "filterValue"],
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "*",
						"groupBy": true
					}]
				},{
					"aggregator": "sum",
					"metric": "http.request",
					"downsample": "1h-min",
					"filterValue": ">10",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": 200,
						"resetValue": 100
					},
					"order": ["filterValue", "aggregation", "downsample", "rate"],
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "*",
						"groupBy": true
					},{
						"type": "literal_or",
						"tagk": "service",
						"filter": "service1|service2",
						"groupBy": false
					}]
				}]
			}`,
			`["groupBy({host=*})|filter(\u003e5,merge(max,downsample(10m,count,null,rate(true,100,0,query(os.cpu,null,5m)))))","groupBy({host=*})|rate(true,200,100,downsample(1h,min,none,merge(sum,filter(\u003e10,query(http.request,{service=or(service1|service2)},5m)))))"]`,
			`["groupBy({host=*})|rate(true,200,100,downsample(1h,min,none,merge(sum,filter(\u003e10,query(http.request,{service=or(service1|service2)},5m)))))","groupBy({host=*})|filter(\u003e5,merge(max,downsample(10m,count,null,rate(true,100,0,query(os.cpu,null,5m)))))"]`,
		},
		"TwoQueriesTagsAndFilters": {
			`{
				"relative": "5m",
				"queries": [{
					"aggregator": "max",
					"metric": "os.cpu",
					"downsample": "10m-count-null",
					"filterValue": ">5",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": 100
					},
					"order": [
						"rate",
						"downsample",
						"aggregation",
						"filterValue"
					],
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "*",
						"groupBy": true
					},{
						"type": "literal_or",
						"tagk": "service",
						"filter": "service1|service2",
						"groupBy": false
					}]
				},{
					"aggregator": "max",
					"metric": "http.request",
					"downsample": "10m-avg-none",
					"tags": {
						"service": "service1"
					}
				}]
			}`,
			`["groupBy({host=*})|filter(\u003e5,merge(max,downsample(10m,count,null,rate(true,100,0,query(os.cpu,{service=or(service1|service2)},5m)))))","merge(max,downsample(10m,avg,none,query(http.request,null,5m)))"]`,
			`["merge(max,downsample(10m,avg,none,query(http.request,null,5m)))","groupBy({host=*})|filter(\u003e5,merge(max,downsample(10m,count,null,rate(true,100,0,query(os.cpu,{service=or(service1|service2)},5m)))))"]`,
		},
	}

	for test, data := range cases {

		statusCode, resp, err := mycenaeTools.HTTP.POST("expression/compile", []byte(data.payload))
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		assert.Equal(t, 200, statusCode, test)
		assert.Condition(t, func() bool { return (data.expression == string(resp)) || (data.expression2 == string(resp)) }, fmt.Sprintf("Found: %v,  Expected: %v or %v", string(resp), data.expression, data.expression2), test)
	}
}

func TestCompileInvalidExpression(t *testing.T) {
	cases := map[string]struct {
		payload string
		err     string
		msg     string
	}{
		"InvalidTagKey": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "*",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"InvalidMetric": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "*",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field metric: *",
			"Invalid characters in field metric: *",
		},
		"InvalidMetric2": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "!@$?%()_+[]^",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field metric: !@$?%()_+[]^",
			"Invalid characters in field metric: !@$?%()_+[]^",
		},
		"DownsampleWithoutMerge": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeInvalidExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "x",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeEmptyExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeNullExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": null,
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"NullFunction": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					null: "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"invalid character 'n' looking for beginning of object key string",
			"Wrong JSON format",
		},
		"DownsampleInvalidPeriod": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "0m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"interval needs to be bigger than 0",
			"interval needs to be bigger than 0",
		},
		"DownsampleInvalidPeriod2": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"DownsampleNullPeriod": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "null-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid unit",
			"Invalid unit",
		},
		"DownsampleEmptyPeriod": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"strconv.Atoi: parsing \"mi\": invalid syntax",
			"strconv.Atoi: parsing \"mi\": invalid syntax",
		},
		"DownsampleInvalidExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-x-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"DownsampleNullExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-null-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"InvalidMergeExpression": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"x": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"RegexMetric": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "*",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field metric: *",
			"Invalid characters in field metric: *",
		},
		"EmptyMetric": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field metric: ",
			"Invalid characters in field metric: ",
		},
		"EmptyTagk": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field tagk: ",
			"Invalid characters in field tagk: ",
		},
		"InvalidTagk": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "!@$?%()_+[]^",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field tagk: !@$?%()_+[]^",
			"Invalid characters in field tagk: !@$?%()_+[]^",
		},
		"EmptyTagV": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field filter: ",
			"Invalid characters in field filter: ",
		},
		"InvalidTagV": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "!@$?%()_+[]^",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid characters in field filter: !@$?%()_+[]^",
			"Invalid characters in field filter: !@$?%()_+[]^",
		},
		"StartAndEnd": {
			`{
				"start": 1448452800000,
				"end": 1448458150000,
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"field relative can not be empty",
			"field relative can not be empty",
		},
		"RelativeAndStartAndEnd": {
			`{
				"relative" : "5m",
				"start": 1448452800000,
				"end": 1448458150000,
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"expression compile supports only relative times, start and end fields should be empty",
			"expression compile supports only relative times, start and end fields should be empty",
		},
		"EmptyRelative": {
			`{
				"relative": "",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"field relative can not be empty",
			"field relative can not be empty",
		},
		"InvalidRelative": {
			`{
				"relative": "5",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"InvalidRelative2": {
			`{
				"relative": "m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"NullRelative": {
			`{
				"relative": null,
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"field relative can not be empty",
			"field relative can not be empty",
		},
		"DownsampleExtraParams": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"MergeExtraParamsRelative": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum-sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"InvalidGroupBy": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": x
					}]
				}]
			}`,
			"invalid character 'x' looking for beginning of value",
			"Wrong JSON format",
		},
		"GroupByRegexTagkAndTagv": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "*",
						"filter": "*",
						"groupBy": true
					}]
				}]
			}`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"GroupByRegexTagk": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "*",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},
		"GroupByNoTags": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"groupBy": true
					}]
				}]
			}`,
			"Invalid characters in field tagk: ",
			"Invalid characters in field tagk: ",
		},
		"InvalidRate": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": ""
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.counter of type bool",
			"Wrong JSON format",
		},
		"RateInvalidCounter": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": "x"
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.counter of type bool",
			"Wrong JSON format",
		},
		"RateEmptyCountermax": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": ""
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.counterMax of type int64",
			"Wrong JSON format",
		},
		"RateInvalidCountermax": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": "x"
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.counterMax of type int64",
			"Wrong JSON format",
		},
		"RateNegativeCountermax": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": true,
						"counterMax": -1
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"counter max needs to be a positive integer",
			"counter max needs to be a positive integer",
		},
		"RateEmptyResetvalue": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": false,
						"counterMax": 1,
						"resetValue": ""
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.resetValue of type int64",
			"Wrong JSON format",
		},
		"RateInvalidResetvalue": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"rate": true,
					"rateOptions": {
						"counter": false,
						"counterMax": 1,
						"resetValue": "x"
					},
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"json: cannot unmarshal string into Go struct field TSDBrateOptions.queries.rateOptions.resetValue of type int64",
			"Wrong JSON format",
		},
		"FilterValueOperator": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"filterValue": "x5",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"invalid filter value x5",
			"invalid filter value x5",
		},
		"FilterValueEmptyOperator": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"filterValue": "5",
					"aggregator": "max",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"invalid filter value 5",
			"invalid filter value 5",
		},
		"FilterValueEmptyNumber": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"downsample": "1m-min-none",
					"filterValue": ">",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": true
					}]
				}]
			}`,
			"invalid filter value >",
			"invalid filter value >",
		},
		"InvalidOrderOperation": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"order": ["aggregation", "downsample", "x"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"invalid operations in order array [x]",
			"invalid operations in order array [x]",
		},
		"OrderOperationNotFound": {
			`{
				"relative": "5m",
				"queries": [{
					"metric": "os.cpu",
					"aggregator": "sum",
					"downsample": "1m-min-none",
					"order": ["aggregation"],
					"filters": [{
						"type": "wildcard",
						"tagk": "app",
						"filter": "test",
						"groupBy":  false
					}]
				}]
			}`,
			"downsample configured but no downsample found in order array",
			"downsample configured but no downsample found in order array",
		},
		"Expression": {
			`x`,
			"invalid character 'x' looking for beginning of value",
			"Wrong JSON format",
		},
		"ExpressionNotSent": {
			``,
			"EOF",
			"Wrong JSON format",
		},
		"ExpressionMetricNotSent": {
			`{
				"relative": "5m",
				"queries": [{
					"downsample": "1m-min-none",
					"aggregator": "sum",
					"filters": [{
						"type": "wildcard",
						"tagk": "host",
						"filter": "test",
						"groupBy": false
					}]
				}]
			}`,
			"Invalid characters in field metric: ",
			"Invalid characters in field metric: ",
		},
	}

	for test, data := range cases {

		statusCode, resp, err := mycenaeTools.HTTP.POST("expression/compile", []byte(data.payload))
		assert.Equal(t, 400, statusCode, test)

		compare := tools.Error{}

		err = json.Unmarshal(resp, &compare)
		if err != nil {
			t.Error(err, test)
			t.SkipNow()
		}

		assert.Equal(t, data.err, compare.Error, test)
		assert.Equal(t, data.msg, compare.Message, test)
	}
}
