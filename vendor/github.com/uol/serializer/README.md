# Serializer

This library provides common data serialization focusing on:

- **performance**
- **memory utilization**
- **low cost cache**

The negative side is to sacrifice some of:

- **usability**
- **flexibility**

## Serializers

The library is organized in subpackages, each subpackage implements a specific data format serialization. 

### JSON

The JSON data format serializer. It uses the same struct tags from the native Go implementation to create the variable to JSON property mappings.

##### Benchmark:
This module was benchmarked against the native implementation and the [JSON Iterator library](github.com/json-iterator/go):
```bash
go test -bench=. -benchmem -benchtime=10s
goos: linux
goarch: amd64
pkg: github.com/uol/serializer/benchmark
BenchmarkNative-4                2000000              7044 ns/op            2608 B/op         52 allocs/op
BenchmarkJSONIter-4              2000000              6547 ns/op            3873 B/op         56 allocs/op
BenchmarkSerializer-4           10000000              2351 ns/op            1328 B/op         16 allocs/op
PASS
ok      github.com/uol/serializer/benchmark     66.903s
```

##### Example:
Consider the following struct:
```Go
type SimpleJSON struct {
	Text    string  `json:"text"`
	Integer int     `json:"integer"`
	Float   float64 `json:"float"`
	Boolean bool    `json:"boolean"`
}
```
To serialize this in the native JSON implementation, it would be ([go playground](https://play.golang.org/p/88u9-QZztDL)):
```Go
s := SimpleJSON{
	Text: "test", 
	Integer: 1, 
	Float: 1.0, 
	Boolean: true,
}
result, _ := json.Marshal(s)
```
To use this module it takes some initial setup and preprocessing:
```Go
import serializer "github.com/uol/serializer/json"
...
jsonSerializer := serializer.New(100) //sets the default string buffer size
```
If your JSON has some constant parts in advance, you can let these parts constant by not defining them as variables:
```Go
s := SimpleJSON{
	Text: "test", 
	Integer: 1, 
	Float: 1.0, 
	Boolean: true,
}

// I want to let the "integer" property constant, so I'll not define it as variable...
jsonSerializer.Add("mySimpleJSON", s, "text", "float", "boolean")

// Ok, now I can serialize it:
result, _ := jsonSerializer.Serialize("mySimpleJSON", 
    "text", "a new text",
    "float", 7.0,
    "boolean", false,
)
```
For more complex examples, please take a look in the tests directory.

### OpenTSDB

The OpenTSDB's data input format serializer. It serializes the line data to send points to a OpenTSDB database telnet listener. The line format can be found in their "Writing Data" page  [here](http://opentsdb.net/docs/build/html/user_guide/writing/index.html).

##### Example:
Some basic point serialization:
```Go
import serializer "github.com/uol/serializer/opentsdb"
...
opentsdbSerializer := serializer.New(100) //sets the default string buffer size
// now we can serialize some data calling function Serialize with parameters: metric, timestamp, value and a list of tags using the format: key, value, key, value...
result, _ := opentsdbSerializer.Serialize("some.metric", time.Now().Unix(), 1.0, "host", "localhost", "number", 1)
```
For more complex examples, please take a look in the tests directory.
