package timeline

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/uol/funks"
	"github.com/uol/logh"
	"github.com/uol/serializer/serializer"
)

/**
* The HTTP transport implementation.
* @author rnojiri
**/

// HTTPTransport - implements the HTTP transport
type HTTPTransport struct {
	core                 transportCore
	httpClient           *http.Client
	serviceURL           string
	configuration        *HTTPTransportConfig
	useCustomJSONMapping bool
	serializer           serializer.Serializer
	serializerTransport  *customSerializerTransport
}

// NewHTTPTransport - creates a new HTTP event manager with a customized serializer
func NewHTTPTransport(configuration *HTTPTransportConfig, customSerializer serializer.Serializer) (*HTTPTransport, error) {

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

	t := &HTTPTransport{
		core: transportCore{
			batchSendInterval:    configuration.BatchSendInterval.Duration,
			defaultConfiguration: &configuration.DefaultTransportConfig,
		},
		serializerTransport: &customSerializerTransport{
			configuration: &configuration.CustomSerializerConfig,
		},
		serializer:    customSerializer,
		configuration: configuration,
		httpClient:    funks.CreateHTTPClient(configuration.RequestTimeout.Duration, true),
	}

	t.core.transport = t

	return t, nil
}

// BuildContextualLogger - build the contextual logger using more info
func (t *HTTPTransport) BuildContextualLogger(path ...string) {

	logContext := []string{"pkg", "timeline/http"}

	if len(path) > 0 {
		logContext = append(logContext, path...)
	}

	t.core.loggers = logh.CreateContextualLogger(logContext...)
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

// SerializePayload - serializes a list of generic data
func (t *HTTPTransport) SerializePayload(dataList []interface{}) (payload []string, err error) {

	serialized, err := t.serializer.SerializeGenericArray(dataList...)
	if err != nil {
		return nil, err
	}

	payload = append(payload, serialized)

	return
}

// DataChannel - send a new point
func (t *HTTPTransport) DataChannel(item interface{}) {

	t.core.dataChannel(item)
}

// TransferData - transfers the data to the backend throught this transport
func (t *HTTPTransport) TransferData(payload []string) error {

	size := len(payload)
	if size == 0 || size > 1 {
		return ErrInvalidPayloadSize
	}

	req, err := http.NewRequest(t.configuration.Method, t.serviceURL, bytes.NewBuffer([]byte(payload[0])))
	if err != nil {
		return err
	}

	if len(t.configuration.Headers) > 0 {
		for k, v := range t.configuration.Headers {
			req.Header.Set(k, v)
		}
	}

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

// Start - starts this transport
func (t *HTTPTransport) Start() error {

	return t.core.Start()
}

// Close - closes this transport
func (t *HTTPTransport) Close() {

	t.core.Close()
}
