package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func TestCheckExpressionValidQuery(t *testing.T) {
	cases := map[string]string{
		"MetricAndTagsWithSpecialChar":        `merge(sum,downsample(30s,min,none,query(os-%&#;_/.cpu,{ap-%&#;_/.p=tes-%&#;_/.t},5m)))`,
		"DownsampleSec":                       `merge(sum,downsample(30s,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleHours":                     `merge(sum,downsample(2h,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleDays":                      `merge(sum,downsample(1d,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleWeeks":                     `merge(sum,downsample(3w,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleMonths":                    `merge(sum,downsample(2n,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleYear":                      `merge(sum,downsample(1y,min,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleMax":                       `merge(sum,downsample(1m,max,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleAvg":                       `merge(sum,downsample(1m,avg,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleSum":                       `merge(sum,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleCount":                     `merge(sum,downsample(1m,count,none,query(os.cpu,{app=test},5m)))`,
		"DownsampleFillNull":                  `groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, null,query(testExpandExpression2, {app=app1}, 5m))))`,
		"DownsampleFillNan":                   `groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, nan,query(testExpandExpression2, {app=app1}, 5m))))`,
		"DownsampleFillZero":                  `groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, zero,query(testExpandExpression2, {app=app1}, 5m))))`,
		"MergeMax":                            `merge(max,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))`,
		"MergeAvg":                            `merge(avg,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))`,
		"MergeMin":                            `merge(min,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))`,
		"MergeCount":                          `merge(count,downsample(1m,sum,none,query(os.cpu,{app=test},5m)))`,
		"FilterValuesGreaterThan":             `merge(max,downsample(1m,sum,none,filter(>5,query(os.cpu,{app=test},5m))))`,
		"FilterValuesGreaterThanEqualTo":      `merge(max,downsample(1m,sum,none,filter(>=5,query(os.cpu,{app=test},5m))))`,
		"FilterValuesLessThan":                `merge(max,downsample(1m,sum,none,filter(<5,query(os.cpu,{app=test},5m))))`,
		"FilterValuesLessThanEqualTo":         `merge(max,downsample(1m,sum,none,filter(<=5,query(os.cpu,{app=test},5m))))`,
		"FilterValuesEqualTo":                 `merge(max,downsample(1m,sum,none,filter(==5,query(os.cpu,{app=test},5m))))`,
		"RelativeSec":                         `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},30s)))`,
		"RelativeHour":                        `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2h)))`,
		"RelativeDay":                         `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1d)))`,
		"RelativeWeek":                        `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},3w)))`,
		"RelativeMonth":                       `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},2n)))`,
		"RelativeYear":                        `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test},1y)))`,
		"MoreThanOneTag":                      `merge(sum,downsample(1m,min,none,query(os.cpu,{app=test,host=host1,cpu=1},5m)))`,
		"NoTags":                              `merge(sum,downsample(1m,min,none,query(os.cpu,null,5m)))`,
		"OrderMergeDownsampleRate":            `rate(false,null,0,downsample(1m,min,none,merge(sum,query(os.cpu,{app=test},5m))))`,
		"OrderMergeRateDownsampleFilterValue": `groupBy({host2=*})|filter(>5, downsample(1m, min, none, rate(false, null, 0, merge(min, query(testExpandExpression2, {app=app1}, 5m)))))`,
		"OrderFilterValueRateMergeDownsample": `groupBy({host2=*})|downsample(1m, min, none, merge(min, rate(false, null, 0, filter(==5, query(testExpandExpression2, {app=app1}, 5m)))))`,
		"OrderRateFilterValueDownsampleMerge": `groupBy({host2=*})|merge(count, downsample(1m, min, null, filter(<= 5, rate(true, 1, 2, query(testExpandExpression2, {app=app1}, 5m)))))`,
		"OrderMergeRateDownsample":            `groupBy({host2=*})|downsample(1m, min, none, rate(false, null, 0, merge(min, query(testExpandExpression2, {app=app1}, 5m))))`,
		"MergeWithoutDownsample":              `merge(sum,query(os.cpu,{app=test},5m))`,
		"GroupbyExpandMatchNoTags":            `groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression, null, 5m))))`,
		"GroupbyExpandMatchWithTags":          `groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))`,
		"GroupbyExpandDontMatch":              `groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(testExpandExpression, {app=test2}, 5m))))`,
		"GroupbyWithoutRate":                  `groupBy({host2=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m)))`,
		"RateTrueNoCounterMaxNoResetValue":    `groupBy({host2=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))`,
		"RateTrueAndCounterMaxNoResetValue":   `groupBy({host2=*})|rate(true, 10, 0, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))`,
		"RateTrueNoCounterMaxAndResetValue":   `groupBy({host2=*})|rate(true, null, 10, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))`,
		"RateTrueCounterMaxAndResetValue":     `groupBy({host2=*})|rate(true, 10, 10, merge(sum, downsample(1m, min,none, query(testExpandExpression2, {app=app1}, 5m))))`,
		"RateFalseCounterMaxAndResetValue":    `groupBy({host2=*})|rate(false, 10, 10, merge(sum, downsample(1m, min, none,query(testExpandExpression2, {app=app1}, 5m))))`,
		"GroupbyTwoTags":                      `groupBy({host2=*, service=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, null, 5m)))`,
		"GroupBySameTagk":                     `groupBy({host2=*, host2=host3})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, null, 5m)))`,
		"SameTagkOnGroupByAndTags":            `groupBy({host2=*})|merge(sum, downsample(1m, min, none,query(testExpandExpression2, {host2=host3}, 5m)))`,
		"SameTagkOrder":                       `merge(sum, downsample(1m,min,none,query(testParseExpression2,{host2=host1,host2=host3,host2=host2,app=app2,app=app3,app=app1}, 5m)))`,
	}

	for test, query := range cases {

		statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`expression/check?exp=%v`, url.QueryEscape(query)))
		assert.Equal(t, 200, statusCode, test)
		assert.Empty(t, resp, test)
	}
}

func TestCheckExpressionInvalidQuery(t *testing.T) {
	cases := map[string]struct {
		expression string
		msg        string
		err        string
	}{
		"DownsampleWithoutMerge": {
			`downsample(1m, min, none,query(os.cpu, {app=test}, 5m))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeInvalidExpression": {
			`merge(x, downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeEmptyExpression": {
			`merge(downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"merge needs 2 parameters: merge operation and a function",
			"merge expects 2 parameters but found 1: [downsample(1m,min,none,query(os.cpu,{app=test},5m))]",
		},
		"MergeNullExpression": {
			`merge(null, downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"unkown aggregation value",
			"unkown aggregation value",
		},
		"MergeInvalidFunction": {
			`merge(sum, x(1m, min, query(os.cpu, {app=test}, 5m)))`,
			"unkown function x",
			"unkown function x",
		},
		"NullFunction": {
			`merge(sum, null(1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"unkown function null",
			"unkown function null",
		},
		"EmptyFunction": {
			`merge(sum, (1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"unkown function ",
			"unkown function ",
		},
		"DuplicateMerge": {
			`merge(sum, merge(min, query(os.cpu, {app=test}, 5m)))`,
			"You can use only one merge function per expression",
			"You can use only one merge function per expression",
		},
		"DuplicateDownsample": {
			`downsample(1m, sum, none,downsample(1m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"You can use only one downsample function per expression",
			"You can use only one downsample function per expression",
		},
		"DownsampleInvalidPeriod": {
			`merge(sum, downsample(0m, min, none,query(os.cpu, {app=test}, 5m)))`,
			"interval needs to be bigger than 0",
			"interval needs to be bigger than 0",
		},
		"DownsampleInvalidPeriod2": {
			`merge(sum, downsample(1, min,none, query(os.cpu, {app=test}, 5m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"DownsampleNullPeriod": {
			`merge(sum, downsample(null, min, none,query(os.cpu, {app=test}, 5m)))`,
			"Invalid unit",
			"Invalid unit",
		},
		"DownsampleEmptyPeriod": {
			`merge(sum, downsample(x, min,none, query(os.cpu, {app=test}, 5m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"DownsampleInvalidExpression": {
			`merge(sum, downsample(1m, x, none,query(os.cpu, {app=test}, 5m)))`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"DownsampleNullExpression": {
			`merge(sum, downsample(1m, null, none,query(os.cpu, {app=test}, 5m)))`,
			"Invalid downsample",
			"Invalid downsample",
		},
		"DownsampleEmptyExpression": {
			`merge(sum, downsample(1m, min,none, x(os.cpu, {app=test}, 5m)))`,
			"unkown function x",
			"unkown function x",
		},
		"NoQuery": {
			`merge(sum, downsample(1m, min,none, (os.cpu, {app=test}, 5m)))`,
			"unkown function ",
			"unkown function ",
		},
		"RegexMetric": {
			`merge(sum, downsample(1m, min,none, query(*, {app=test}, 5m)))`,
			"Invalid characters in field metric: *",
			"Invalid characters in field metric: *",
		},
		"EmptyMetric": {
			`merge(sum, downsample(1m, min,none,query({app=test}, 5m)))`,
			"query needs 3 parameters: metric, map or null and a time interval",
			"query expects 3 parameters but found 2: [{app=test} 5m]",
		},
		"EmptyTag": {
			`merge(sum, downsample(1m, min,none, query(os.cpu, , 5m)))`,
			"empty map",
			"empty map",
		},
		"EmptyTagValue": {
			`merge(sum, downsample(1m, min,none,query(os.cpu, {app=}, 5m)))`,
			"map value cannot be empty",
			"map value cannot be empty",
		},
		"EmptyTagKey": {
			`merge(sum, downsample(1m, min, none,query(os.cpu, {=test}, 5m)))`,
			"map key cannot be empty",
			"map key cannot be empty",
		},
		"EmptyRelative": {
			`merge(sum, downsample(1m, min,none, query(os.cpu, {app=test})))`,
			"query needs 3 parameters: metric, map or null and a time interval",
			"query expects 3 parameters but found 2: [os.cpu {app=test}]",
		},
		"InvalidRelative": {
			`merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"InvalidRelative2": {
			`merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, m)))`,
			"Invalid time interval",
			"Invalid time interval",
		},
		"NullRelative": {
			`merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, null)))`,
			"Invalid unit",
			"Invalid unit",
		},
		"DownsampleExtraParamsRelative": {
			`merge(sum, downsample(1m ,1m, min,none,query(os.cpu, {app=test}, 5m)))`,
			"downsample needs 4 parameters: downsample operation, downsample period, fill option and a function",
			"downsample expects 4 parameters but found 5: [1m 1m min none query(os.cpu,{app=test},5m)]",
		},
		"MergeExtraParamsRelative": {
			`merge(sum, sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m)))`,
			"merge needs 2 parameters: merge operation and a function",
			"merge expects 2 parameters but found 3: [sum sum downsample(1m,min,none,query(os.cpu,{app=test},5m))]",
		},
		"Expression": {
			`x`,
			"unkown function x",
			"unkown function x",
		},
		"EmptyExpression": {
			``,
			"no expression found",
			"no expression found",
		},
		"InvalidGroupBy": {
			`x|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`,
			"unkown function x|rate",
			"unkown function x|rate",
		},
		"GroupByRegexTagkAndTagv": {
			`groupBy({*=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},

		"GroupByRegexTagk": {
			`groupBy({*=host})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"Invalid characters in field tagk: *",
			"Invalid characters in field tagk: *",
		},

		"GroupByNoTags": {
			`groupBy({})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`,
			"bad map format",
			"bad map format",
		},

		"EmptyGroupBy": {
			`groupBy()|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"empty map",
			"empty map",
		},

		"InvalidRate": {
			`rate(, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counter, the 1st parameter, needs to be a boolean",
			"strconv.ParseBool: parsing \"\": invalid syntax",
		},

		"NullRate": {
			`rate(null, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counter, the 1st parameter, needs to be a boolean",
			"strconv.ParseBool: parsing \"null\": invalid syntax",
		},

		"RateEmptyCounter": {
			`rate(, null , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counter, the 1st parameter, needs to be a boolean",
			"strconv.ParseBool: parsing \"\": invalid syntax",
		},

		"Counter": {
			`rate("x", null , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counter, the 1st parameter, needs to be a boolean",
			"strconv.ParseBool: parsing \"\\\"x\\\"\": invalid syntax",
		},

		"RateEmptyCountermax": {
			`rate(true, , 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
			"strconv.ParseInt: parsing \"\": invalid syntax",
		},

		"RateInvalidCountermax": {
			`rate(true, x, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate counterMax, the 2nd parameter, needs to be an integer or the string 'null'",
			"strconv.ParseInt: parsing \"x\": invalid syntax",
		},

		"RateNegativeCountermax": {
			`rate(true, -1, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`,
			"counter max needs to be a positive integer",
			"counter max needs to be a positive integer",
		},

		"RateInvalidCountermax2": {
			`rate(true, -1, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"counter max needs to be a positive integer",
			"counter max needs to be a positive integer",
		},

		"RateEmptyResetvalue": {
			`rate(true, null, , merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate resetValue, the 3rd parameter, needs to be an integer",
			"strconv.ParseInt: parsing \"\": invalid syntax",
		},

		"RateNullResetvalue": {
			`rate(true, null, null, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`,
			"rate resetValue, the 3rd parameter, needs to be an integer",
			"strconv.ParseInt: parsing \"null\": invalid syntax",
		},

		"RateInvalidResetvalue": {
			`rate(true, null, x, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`,
			"rate resetValue, the 3rd parameter, needs to be an integer",
			"strconv.ParseInt: parsing \"x\": invalid syntax",
		},

		"FilterValueOperator": {
			`merge(max,downsample(1m,sum,none,filter(x5,query(os.cpu,{app=test},5m))))`,
			"invalid filter value x5",
			"invalid filter value x5",
		},

		"FilterValueEmptyOperator": {
			`merge(max,downsample(1m,sum,none,filter(5,query(os.cpu,{app=test},5m))))`,
			"invalid filter value 5",
			"invalid filter value 5",
		},

		"FilterValueEmptyNumber": {
			`merge(max,downsample(1m,sum,none,filter(>,query(os.cpu,{app=test},5m))))`,
			"invalid filter value >",
			"invalid filter value >",
		},
	}

	for test, data := range cases {

		statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`expression/check?exp=%v`, url.QueryEscape(data.expression)))
		assert.Equal(t, 400, statusCode, test)

		compare := tools.Error{}

		err := json.Unmarshal(resp, &compare)
		if err != nil {
			t.Error(err)
			t.SkipNow()
		}

		assert.Equal(t, data.msg, compare.Message, test)
		assert.Equal(t, data.err, compare.Error, test)
	}
}

func TestCheckExpressionQueryExpressionNotSent(t *testing.T) {

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keysets/%v/expression/expand`, ksMycenae))
	assert.Equal(t, 400, statusCode)

	compare := tools.Error{}
	err := json.Unmarshal(resp, &compare)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, "no expression found", compare.Error)
	assert.Equal(t, "no expression found", compare.Message)
}

func TestCheckExpressionInvalidQueryGroupByKeyspaceNotFound(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min, none,query(os.cpu, {app=test}, 5m))))`)

	statusCode, resp, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keysets/fake/expression/expand?exp=%v`, expression))
	assert.Equal(t, 404, statusCode)
	assert.Equal(t, 0, len(resp))
}

func TestCheckExpressionInvalidQueryGroupByKeyspaceNotSent(t *testing.T) {

	expression := url.QueryEscape(
		`groupBy({host=*})|rate(true, null, 0, merge(sum, downsample(1m, min,none, query(os.cpu, {app=test}, 5m))))`)

	statusCode, _, _ := mycenaeTools.HTTP.GET(fmt.Sprintf(`keysets/expression/expand?exp=%v`, expression))
	assert.Equal(t, 404, statusCode)

}
