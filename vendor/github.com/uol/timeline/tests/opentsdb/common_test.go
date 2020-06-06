package timeline_opentsdb_test

import (
	"time"

	"github.com/uol/funks"
	gotesttelnet "github.com/uol/gotest/telnet"
	serializer "github.com/uol/serializer/opentsdb"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

var (
	defaultConf gotesttelnet.Configuration = gotesttelnet.Configuration{
		Host:               "localhost",
		MessageChannelSize: 10,
		ReadBufferSize:     512,
		ReadTimeout:        3 * time.Second,
	}
)

const (
	timeBetweenBatches int = 50
)

// createOpenTSDBTransport - creates the http transport
func createOpenTSDBTransport(transportBufferSize int, batchSendInterval time.Duration) *timeline.OpenTSDBTransport {

	transportConf := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			BatchSendInterval: funks.Duration{
				Duration: batchSendInterval,
			},
			RequestTimeout: funks.Duration{
				Duration: time.Second,
			},
			TransportBufferSize:  transportBufferSize,
			SerializerBufferSize: 1024,
			TimeBetweenBatches:   funks.Duration{Duration: time.Duration(timeBetweenBatches) * time.Millisecond},
		},
		ReadBufferSize: 64,
		MaxReadTimeout: funks.Duration{
			Duration: time.Second,
		},
		ReconnectionTimeout: funks.Duration{
			Duration: time.Second,
		},
	}

	transport, err := timeline.NewOpenTSDBTransport(&transportConf)
	if err != nil {
		panic(err)
	}

	return transport
}

// newArrayItem - creates a new array item
func newArrayItem(metric string, value float64) serializer.ArrayItem {

	return serializer.ArrayItem{
		Metric:    metric,
		Timestamp: time.Now().Unix(),
		Value:     value,
		Tags: []interface{}{
			"tagk1", "tagv1",
			"tagk2", "tagv2",
			"tagk3", "tagv3",
		},
	}
}
