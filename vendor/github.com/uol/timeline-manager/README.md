# Timeline Manager
This library manages multiple timeline instances to allow the use of multiple backends and provides useful shortcut functions to manipulate these timelines.

## Timeline library?
[Click here](https://github.com/uol/timeline)

## Sample TOML
~~~
hashingAlgorithm = "shake128"
hashSize = 6
transportBufferSize = 10000
serializerBufferSize = 2048
batchSendInterval = "30s"
requestTimeout = "5s"
maxReadTimeout = "100ms"
reconnectionTimeout = "3s"
dataTTL = "2m"
readBufferSize = 64
debugOutput = true
debugInput = true

[[stats.backends]]
host = "loghost"
port = 8123
type = "normal"
cycleDuration = "15s"
addHostTag = true
[stats.backends.commonTags]
    ttl = "1"
    ksid = "normal"

[[stats.backends]]
host = "loghost"
port = 8123
type = "archive"
cycleDuration = "15s"
addHostTag = true
[stats.backends.commonTags]
    ttl = "1"
    ksid = "storage"