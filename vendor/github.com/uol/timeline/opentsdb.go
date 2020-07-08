package timeline

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/uol/logh"
	serializer "github.com/uol/serializer/opentsdb"
)

/**
* The OpenTSDB transport implementation.
* @author rnojiri
**/

// OpenTSDBTransport - implements the openTSDB transport
type OpenTSDBTransport struct {
	core           transportCore
	configuration  *OpenTSDBTransportConfig
	serializer     *serializer.Serializer
	address        *net.TCPAddr
	tcpNetworkConn *rawNetworkConnection
}

// NewOpenTSDBTransport - creates a new openTSDB event manager
func NewOpenTSDBTransport(configuration *OpenTSDBTransportConfig) (*OpenTSDBTransport, error) {

	if configuration == nil {
		return nil, fmt.Errorf("null configuration found")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	if configuration.ReadBufferSize <= 0 {
		return nil, fmt.Errorf("invalid read buffer size: %d", configuration.ReadBufferSize)
	}

	if configuration.MaxReadTimeout.Seconds() <= 0 {
		return nil, fmt.Errorf("invalid connection maximum read timeout: %s", configuration.MaxReadTimeout)
	}

	if configuration.ReconnectionTimeout.Seconds() <= 0 {
		return nil, fmt.Errorf("invalid connection reconnection timeout: %s", configuration.ReconnectionTimeout)
	}

	if configuration.MaxReconnectionRetries == 0 {
		configuration.MaxReconnectionRetries = defaultConnRetries
	}

	s := serializer.New(configuration.SerializerBufferSize)

	t := &OpenTSDBTransport{
		core: transportCore{
			batchSendInterval:    configuration.BatchSendInterval.Duration,
			defaultConfiguration: &configuration.DefaultTransportConfig,
		},
		tcpNetworkConn: &rawNetworkConnection{
			transportConfiguration: &configuration.DefaultTransportConfig,
			configuration:          &configuration.TCPUDPTransportConfig,
		},
		configuration: configuration,
		serializer:    s,
	}

	t.core.transport = t
	t.tcpNetworkConn.custom = t

	return t, nil
}

// BuildContextualLogger - build the contextual logger using more info
func (t *OpenTSDBTransport) BuildContextualLogger(path ...string) {

	logContext := []string{"pkg", "timeline/opentsdb"}

	if len(path) > 0 {
		logContext = append(logContext, path...)
	}

	t.core.loggers = logh.CreateContextualLogger(logContext...)
	t.tcpNetworkConn.loggers = t.core.loggers
}

// ConfigureBackend - configures the backend
func (t *OpenTSDBTransport) ConfigureBackend(backend *Backend) error {

	if backend == nil {
		return fmt.Errorf("no backend was configured")
	}

	var err error
	t.address, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", backend.Host, backend.Port))
	if err != nil {
		return err
	}

	return nil
}

// SerializePayload - serializes a list of generic data
func (t *OpenTSDBTransport) SerializePayload(dataList []interface{}) (payload []string, err error) {

	serialized, err := t.serializer.SerializeGenericArray(dataList...)
	if err != nil {
		return nil, err
	}

	payload = append(payload, serialized)

	return
}

func (t *OpenTSDBTransport) getAddress() net.Addr {

	return t.address
}

func (t *OpenTSDBTransport) read(conn net.Conn, logConnError func(error, rwOp)) bool {

	err := conn.SetReadDeadline(time.Now().Add(t.configuration.MaxReadTimeout.Duration))
	if err != nil {
		if logh.ErrorEnabled {
			ev := t.core.loggers.Error()
			if t.core.defaultConfiguration.PrintStackOnError {
				ev = ev.Caller()
			}
			ev.Err(err).Msg("error setting read deadline")
		}
		return false
	}

	readBuffer := make([]byte, t.configuration.ReadBufferSize)
	_, err = conn.Read(readBuffer)
	if err != nil {
		if err == io.EOF {
			logConnError(err, readConnClosed)
			return false
		}

		if castedErr, ok := err.(net.Error); ok && !castedErr.Timeout() {
			logConnError(err, read)
			return false
		}
	}

	return true
}

func (t *OpenTSDBTransport) dial() (net.Conn, error) {

	return net.DialTCP("tcp", nil, t.address)
}

// TransferData - transfers the data to the backend throught this transport
func (t *OpenTSDBTransport) TransferData(payload []string) error {

	size := len(payload)
	if size == 0 || size > 1 {
		return ErrInvalidPayloadSize
	}

	return t.tcpNetworkConn.transferData(payload[0])
}

// DataChannel - send a new point
func (t *OpenTSDBTransport) DataChannel(item interface{}) {

	t.core.dataChannel(item)
}

// MatchType - checks if this transport implementation matches the given type
func (t *OpenTSDBTransport) MatchType(tt transportType) bool {

	return tt == typeOpenTSDB
}

// Start - starts this transport
func (t *OpenTSDBTransport) Start() error {

	return t.core.Start()
}

// Close - closes this transport
func (t *OpenTSDBTransport) Close() {

	t.core.Close()
	t.tcpNetworkConn.closeConnection()
}
