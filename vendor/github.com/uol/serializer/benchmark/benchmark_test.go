package serializer_benchmark_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/uol/gobol/structs"
	serializer "github.com/uol/serializer/json"

	jsoniter "github.com/json-iterator/go"
)

/**
* Some benchmark tests.
* @author rnojiri
**/

var jsonIter = jsoniter.ConfigCompatibleWithStandardLibrary
var numbers = []structs.NumberPoint{
	structs.NumberPoint{
		Point: structs.Point{
			Metric:    "metric1",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "here",
				"ttl":  "1",
			},
		},
		Value: 1.0,
	},

	structs.NumberPoint{
		Point: structs.Point{
			Metric:    "metric2",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "here",
				"ttl":  "1",
			},
		},
		Value: 2.0,
	},
}

var texts = []structs.TextPoint{
	structs.TextPoint{
		Point: structs.Point{
			Metric:    "metric1",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "here",
				"ttl":  "1",
			},
		},
		Text: "test1",
	},

	structs.TextPoint{
		Point: structs.Point{
			Metric:    "metric2",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"ksid": "keyset",
				"host": "here",
				"ttl":  "1",
			},
		},
		Text: "test2",
	},
}

func BenchmarkNative(b *testing.B) {
	for n := 0; n < b.N; n++ {
		json.Marshal(numbers)
		json.Marshal(texts)
	}
}

func BenchmarkJSONIter(b *testing.B) {
	for n := 0; n < b.N; n++ {
		jsonIter.Marshal(numbers)
		jsonIter.Marshal(texts)
	}
}

func BenchmarkSerializer(b *testing.B) {
	s := serializer.New(100)
	s.Add("n", numbers[0], "metric", "value")
	s.Add("t", texts[0], "metric", "text")

	for n := 0; n < b.N; n++ {
		s.SerializeArray([]serializer.ArrayItem{
			serializer.ArrayItem{Name: "n", Parameters: []interface{}{"metric", "number", "value", 1.0}},
			serializer.ArrayItem{Name: "n", Parameters: []interface{}{"metric", "number", "value", 2.0}},
		}...)
		s.SerializeArray([]serializer.ArrayItem{
			serializer.ArrayItem{Name: "t", Parameters: []interface{}{"metric", "text", "text", "1.0"}},
			serializer.ArrayItem{Name: "t", Parameters: []interface{}{"metric", "text", "text", "2.0"}},
		}...)
	}
}
