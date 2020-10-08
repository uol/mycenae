package timelinemanager

import (
	"fmt"

	"github.com/uol/funks"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
)

// StorageType - the storage type constant
type StorageType string

// TransportType - the transport type constant
type TransportType string

// SerializerType - the serializer type constant
type SerializerType string

const (
	// NormalStorage - normal storage backend
	NormalStorage StorageType = "normal"

	// ArchiveStorage - archive storage backend
	ArchiveStorage StorageType = "archive"

	// HTTPTransport - http transport type
	HTTPTransport TransportType = "http"

	// OpenTSDBTransport - opentsdb transport type
	OpenTSDBTransport TransportType = "opentsdb"

	// UDPTransport - udp transport type
	UDPTransport TransportType = "udp"

	// JSONSerializer - json serializer type
	JSONSerializer SerializerType = "json"

	// OpenTSDBSerializer - opentsdb serializer type
	OpenTSDBSerializer SerializerType = "opentsdb"

	cFunction  string = "func"
	cType      string = "type"
	cOperation string = "operation"
	cHost      string = "host"

	cHTTPNumberFormat string = "httpNumberFormat"
	cHTTPTextFormat   string = "httpTextFormat"

	cLoggerStorage string = "storage"
)

// ErrStorageNotFound - raised when a storage type was not found
var ErrStorageNotFound error = fmt.Errorf("storage type not found")

// ErrTransportNotSupported - raised when a transport is not supported for the specified storage
var ErrTransportNotSupported error = fmt.Errorf("transport not supported")

// BackendItem - one backend configuration
type BackendItem struct {
	timeline.Backend
	Storage       StorageType       `json:"storage,omitempty"`
	CycleDuration funks.Duration    `json:"cycleDuration,omitempty"`
	AddHostTag    bool              `json:"addHostTag,omitempty"`
	CommonTags    map[string]string `json:"commonTags,omitempty"`
	Transport     string            `json:"transport,omitempty"`
	transportType TransportType
}

// CustomJSONMapping - a custom json mapping to be added
type CustomJSONMapping struct {
	MappingName string      `json:"mappingName,omitempty"`
	Instance    interface{} `json:"instance,omitempty"`
	Variables   []string    `json:"variables,omitempty"`
}

// TransportExt - an transport extension
type TransportExt struct {
	Serializer   SerializerType      `json:"serializer,omitempty"`
	JSONMappings []CustomJSONMapping `json:"jsonMappings,omitempty"`
}

// HTTPTransportConfigExt - an extension to the timeline.HTTPTransportConfig
type HTTPTransportConfigExt struct {
	TransportExt
	timeline.HTTPTransportConfig
}

// UDPTransportConfigExt - an extension to the timeline.UDPTransportConfig
type UDPTransportConfigExt struct {
	TransportExt
	timeline.UDPTransportConfig
}

// OpenTSDBTransportConfigExt - an extension to the timeline.OpenTSDBTransportConfig
type OpenTSDBTransportConfigExt struct {
	timeline.OpenTSDBTransportConfig
}

// Configuration - configuration
type Configuration struct {
	Backends         []BackendItem     `json:"backends,omitempty"`
	HashingAlgorithm hashing.Algorithm `json:"hashingAlgorithm,omitempty"`
	HashSize         int               `json:"hashSize,omitempty"`
	DataTTL          funks.Duration    `json:"dataTTL,omitempty"`
	timeline.DefaultTransportConfig
	OpenTSDBTransports map[string]OpenTSDBTransportConfigExt `json:"openTSDBTransports,omitempty"`
	HTTPTransports     map[string]HTTPTransportConfigExt     `json:"httpTransports,omitempty"`
	UDPTransports      map[string]UDPTransportConfigExt      `json:"udpTransports,omitempty"`
}

// Validate - validates the configuration
func (c *Configuration) Validate() error {

	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}

	var hasOpenTSDB, hasHTTP, hasUDP bool

	if hasOpenTSDB = len(c.OpenTSDBTransports) > 0; hasOpenTSDB {
		for k := range c.OpenTSDBTransports {
			v := c.OpenTSDBTransports[k]
			v.OpenTSDBTransportConfig.DefaultTransportConfig = c.DefaultTransportConfig
			c.OpenTSDBTransports[k] = v
		}
	}

	if hasHTTP = len(c.HTTPTransports) > 0; hasHTTP {
		for k := range c.HTTPTransports {
			v := c.HTTPTransports[k]
			v.HTTPTransportConfig.DefaultTransportConfig = c.DefaultTransportConfig
			c.HTTPTransports[k] = v
		}
	}

	if hasUDP = len(c.UDPTransports) > 0; hasUDP {
		for k := range c.UDPTransports {
			v := c.UDPTransports[k]
			v.UDPTransportConfig.DefaultTransportConfig = c.DefaultTransportConfig
			c.UDPTransports[k] = v
		}
	}

	if !hasOpenTSDB && !hasHTTP && !hasUDP {
		return fmt.Errorf("no transports configured")
	}

	return nil
}
