package timeline

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/uol/gobol/logh"
	"github.com/uol/gobol/util"
	serializer "github.com/uol/serializer/json"
)

/**
* The HTTP JSON transport implementation.
* @author rnojiri
**/

// HTTPTransport - implements the HTTP transport
type HTTPTransport struct {
	core                 transportCore
	httpClient           *http.Client
	serviceURL           string
	configuration        *HTTPTransportConfig
	serializer           *serializer.Serializer
	useCustomJSONMapping bool
}

// HTTPTransportConfig - has all HTTP event manager configurations
type HTTPTransportConfig struct {
	DefaultTransportConfiguration
	ServiceEndpoint        string
	Method                 string
	ExpectedResponseStatus int
	TimestampProperty      string
	ValueProperty          string
}

// NewHTTPTransport - creates a new HTTP event manager
func NewHTTPTransport(configuration *HTTPTransportConfig) (*HTTPTransport, error) {

	if configuration == nil {
		return nil, fmt.Errorf("null configuration found")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	if len(configuration.TimestampProperty) == 0 {
		return nil, fmt.Errorf("timestamp property is not configured")
	}

	if len(configuration.ValueProperty) == 0 {
		return nil, fmt.Errorf("value property is not configured")
	}

	s := serializer.New(configuration.SerializerBufferSize)

	t := &HTTPTransport{
		core: transportCore{
			batchSendInterval: configuration.BatchSendInterval,
			pointChannel:      make(chan interface{}, configuration.TransportBufferSize),
			loggers:           logh.CreateContextualLogger("pkg", "timeline/http"),
		},
		configuration: configuration,
		httpClient:    util.CreateHTTPClient(configuration.RequestTimeout, true),
		serializer:    s,
	}

	t.core.transport = t

	return t, nil
}

// AddJSONMapping - overrides the default generic property mappings
func (t *HTTPTransport) AddJSONMapping(name string, p interface{}, variables ...string) error {

	return t.serializer.Add(name, p, variables...)
}

// ConfigureBackend - configures the backend
func (t *HTTPTransport) ConfigureBackend(backend *Backend) error {

	if backend == nil {
		return fmt.Errorf("no backend was configured")
	}

	t.serviceURL = fmt.Sprintf("http://%s:%d/%s", backend.Host, backend.Port, t.configuration.ServiceEndpoint)

	if logh.InfoEnabled {
		t.core.loggers.Info().Msg(fmt.Sprintf("backend was configured to use service: %s", t.serviceURL))
	}

	return nil
}

// DataChannel - send a new point
func (t *HTTPTransport) DataChannel() chan<- interface{} {

	return t.core.pointChannel
}

// TransferData - transfers the data to the backend throught this transport
func (t *HTTPTransport) TransferData(dataList []interface{}) error {

	numPoints := len(dataList)
	points := make([]serializer.ArrayItem, numPoints)
	var ok bool
	for i := 0; i < numPoints; i++ {
		points[i], ok = dataList[i].(serializer.ArrayItem)
		if !ok {
			return fmt.Errorf("error casting data to serializer.ArrayItem")
		}
	}

	payload, err := t.serializer.SerializeArray(points...)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(t.configuration.Method, t.serviceURL, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-type", "application/json")

	res, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != t.configuration.ExpectedResponseStatus {

		reqResponse, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading body: %s", err.Error())
		}

		return fmt.Errorf("error body: %s", string(reqResponse))
	}

	res.Body.Close()

	return nil
}

// MatchType - checks if this transport implementation matches the given type
func (t *HTTPTransport) MatchType(tt transportType) bool {

	return tt == typeHTTP
}

// DataChannelItemToFlattenedPoint - converts the data channel item to the flattened point one
func (t *HTTPTransport) DataChannelItemToFlattenedPoint(operation FlatOperation, instance interface{}) (*FlattenerPoint, error) {

	item, ok := instance.(*serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting instance to data channel item")
	}

	hashParameters := []interface{}{}
	hashParameters = append(hashParameters, item.Name, operation)

	valueFound := false
	timestampFound := false
	var value float64
	var timestamp int64

	for i := 0; i < len(item.Parameters); i++ {

		if i%2 == 0 && (!valueFound || !timestampFound) {

			key, ok := item.Parameters[i].(string)
			if !ok {
				return nil, fmt.Errorf("expecting a property name in parameter item: %s", item.Parameters[i])
			}

			if !valueFound && key == t.configuration.ValueProperty {
				valueFound = true
				value, ok = item.Parameters[i+1].(float64)
				if !ok {
					return nil, fmt.Errorf("expecting a float64 as value for parameter: %s", item.Parameters[i+1])
				}

				i++
				continue
			}

			if !timestampFound && key == t.configuration.TimestampProperty {
				timestampFound = true
				timestamp, ok = item.Parameters[i+1].(int64)
				if !ok {
					return nil, fmt.Errorf("expecting a int64 as value for parameter: %s", item.Parameters[i+1])
				}

				i++
				continue
			}
		}

		hashParameters = append(hashParameters, item.Parameters[i])
	}

	if !timestampFound {
		timestamp = time.Now().Unix()
	}

	return &FlattenerPoint{
		value:          value,
		hashParameters: hashParameters,
		flattenerPointData: flattenerPointData{
			operation: operation,
			timestamp: timestamp,
			dataChannelItem: serializer.ArrayItem{
				Name:       item.Name,
				Parameters: hashParameters[2:],
			},
		},
	}, nil
}

// FlattenedPointToDataChannelItem - converts the flattened point to the data channel one
func (t *HTTPTransport) FlattenedPointToDataChannelItem(point *FlattenerPoint) (interface{}, error) {

	item, ok := point.dataChannelItem.(serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting point's data channel item")
	}

	item.Parameters = append(item.Parameters, t.configuration.TimestampProperty, point.timestamp, t.configuration.ValueProperty, point.value)

	return item, nil
}

// Start - starts this transport
func (t *HTTPTransport) Start() error {

	return t.core.Start()
}

// Close - closes this transport
func (t *HTTPTransport) Close() {

	t.core.Close()
}

// Serialize - renders the text using the configured serializer
func (t *HTTPTransport) Serialize(item interface{}) (string, error) {

	return t.serializer.SerializeGeneric(item)
}
