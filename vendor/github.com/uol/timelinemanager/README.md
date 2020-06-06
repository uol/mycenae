# Timeline Manager
This library manages multiple timeline instances to allow the use of multiple backends and provides useful shortcut functions to manipulate these timelines.

## Timeline library?
[Click here](https://github.com/uol/timeline)

## Sample TOML
~~~
hashingAlgorithm = "shake128"
hashSize = 6
dataTTL = "2m"
transportBufferSize = 1024
batchSendInterval = "30s"
requestTimeout = "5s"
serializerBufferSize = 2048
debugInput = false
debugOutput = true
timeBetweenBatches = "10ms"
printStackOnError = true

[openTSDBTransport]
    readBufferSize = 64
    maxReadTimeout = "100ms"
    reconnectionTimeout = "3s"
    maxReconnectionRetries = 5
    disconnectAfterWrites = true

[httpTransport]
    serviceEndpoint = "/api/put"
    method = "POST"
    expectedResponseStatus = 204
    timestampProperty = "timestamp"
    valueProperty = "value"

[[backends]]
    addHostTag    = true
    cycleDuration = "15s"
    host          = "host1"
    port          = 8123
    storage       = "normal"
    type          = "opentsdb"
        [backends.commonTags]
        tag1 = "val1"
        tag2 = "val2"
        tag3 = "val3"

[[backends]]
    addHostTag    = true
    cycleDuration = "25s"
    host          = "host2"
    port          = 8124
    storage       = "archive"
    type          = "opentsdb"
        [backends.commonTags]
        tag4 = "val4"
        tag5 = "val5"
        tag6 = "val6"

[[backends]]
    addHostTag    = false
    cycleDuration = "35s"
    host          = "host3"
    port          = 8125
    storage       = "custom"
    type          = "http"
        [backends.commonTags]
        tag7 = "val7"
        tag8 = "val8"
        tag9 = "val9"
