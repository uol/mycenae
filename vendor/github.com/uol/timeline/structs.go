package timeline

import (
	"github.com/uol/funks"
	"github.com/uol/hashing"
)

/**
* All exported structs used by the timeline library.
* @author rnojiri
**/

// Backend - the destiny opentsdb backend
type Backend struct {
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
}

// DataTransformerConfig - flattener configuration
type DataTransformerConfig struct {
	CycleDuration     funks.Duration    `json:"cycleDuration,omitempty"`
	HashingAlgorithm  hashing.Algorithm `json:"hashingAlgorithm,omitempty"`
	HashSize          int               `json:"hashSize,omitempty"`
	PrintStackOnError bool              `json:"printStackOnError,omitempty"`
	isSHAKE           bool
}

// DefaultTransportConfig - the default fields used by the transport configuration
type DefaultTransportConfig struct {
	TransportBufferSize  int            `json:"transportBufferSize,omitempty"`
	BatchSendInterval    funks.Duration `json:"batchSendInterval,omitempty"`
	RequestTimeout       funks.Duration `json:"requestTimeout,omitempty"`
	SerializerBufferSize int            `json:"serializerBufferSize,omitempty"`
	DebugInput           bool           `json:"debugInput,omitempty"`
	DebugOutput          bool           `json:"debugOutput,omitempty"`
	TimeBetweenBatches   funks.Duration `json:"timeBetweenBatches,omitempty"`
	PrintStackOnError    bool           `json:"printStackOnError,omitempty"`
}

// CustomSerializerConfig - configures a customized serialization transport
type CustomSerializerConfig struct {
	TimestampProperty string `json:"timestampProperty,omitempty"`
	ValueProperty     string `json:"valueProperty,omitempty"`
}

// HTTPTransportConfig - has all http transport configurations
type HTTPTransportConfig struct {
	DefaultTransportConfig
	ServiceEndpoint        string            `json:"serviceEndpoint,omitempty"`
	Method                 string            `json:"method,omitempty"`
	ExpectedResponseStatus int               `json:"expectedResponseStatus,omitempty"`
	Headers                map[string]string `json:"headers,omitempty"`
	CustomSerializerConfig
}

// TCPUDPTransportConfig - defines some common parameters for a tcp/udp connection
type TCPUDPTransportConfig struct {
	ReconnectionTimeout    funks.Duration `json:"reconnectionTimeout,omitempty"`
	MaxReconnectionRetries int            `json:"maxReconnectionRetries,omitempty"`
	DisconnectAfterWrites  bool           `json:"disconnectAfterWrites,omitempty"`
}

// OpenTSDBTransportConfig - has all opentsdb transport configurations
type OpenTSDBTransportConfig struct {
	DefaultTransportConfig
	ReadBufferSize int            `json:"readBufferSize,omitempty"`
	MaxReadTimeout funks.Duration `json:"maxReadTimeout,omitempty"`
	TCPUDPTransportConfig
}

// UDPTransportConfig - has all udp transport configurations
type UDPTransportConfig struct {
	DefaultTransportConfig
	TCPUDPTransportConfig
	CustomSerializerConfig
}
