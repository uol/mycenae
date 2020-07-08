package timeline

import (
	"fmt"
	"net"

	"github.com/uol/logh"
	"github.com/uol/serializer/serializer"
)

/**
* The UDP transport implementation.
* @author rnojiri
**/

// UDPTransport - implements the UDP transport
type UDPTransport struct {
	core                transportCore
	configuration       *UDPTransportConfig
	serializer          serializer.Serializer
	address             *net.UDPAddr
	udpNetworkConn      *rawNetworkConnection
	serializerTransport *customSerializerTransport
}

// NewUDPTransport - creates a new HTTP event manager with a customized serializer
func NewUDPTransport(configuration *UDPTransportConfig, customSerializer serializer.Serializer) (*UDPTransport, error) {

	if configuration == nil {
		return nil, fmt.Errorf("null configuration found")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	t := &UDPTransport{
		core: transportCore{
			batchSendInterval:    configuration.BatchSendInterval.Duration,
			defaultConfiguration: &configuration.DefaultTransportConfig,
		},
		udpNetworkConn: &rawNetworkConnection{
			transportConfiguration: &configuration.DefaultTransportConfig,
			configuration:          &configuration.TCPUDPTransportConfig,
		},
		serializerTransport: &customSerializerTransport{
			configuration: &configuration.CustomSerializerConfig,
		},
		configuration: configuration,
		serializer:    customSerializer,
	}

	t.core.transport = t
	t.udpNetworkConn.custom = t

	return t, nil
}

// BuildContextualLogger - build the contextual logger using more info
func (t *UDPTransport) BuildContextualLogger(path ...string) {

	logContext := []string{"pkg", "timeline/udp"}

	if len(path) > 0 {
		logContext = append(logContext, path...)
	}

	t.core.loggers = logh.CreateContextualLogger(logContext...)
	t.udpNetworkConn.loggers = t.core.loggers
}

// ConfigureBackend - configures the backend
func (t *UDPTransport) ConfigureBackend(backend *Backend) error {

	if backend == nil {
		return fmt.Errorf("no backend was configured")
	}

	var err error
	t.address, err = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", backend.Host, backend.Port))
	if err != nil {
		return err
	}

	return nil
}

// SerializePayload - serializes a list of generic data
func (t *UDPTransport) SerializePayload(dataList []interface{}) (payload []string, err error) {

	size := len(dataList)
	payload = make([]string, size)

	for i := 0; i < size; i++ {

		serialized, err := t.serializer.SerializeGeneric(dataList[i])
		if err != nil {
			return nil, err
		}

		payload[i] = serialized
	}

	return
}

func (t *UDPTransport) getAddress() net.Addr {

	return t.address
}

func (t *UDPTransport) read(conn net.Conn, logConnError func(error, rwOp)) bool {

	return true
}

func (t *UDPTransport) dial() (net.Conn, error) {

	return net.DialUDP("udp", nil, t.address)
}

// TransferData - transfers the data to the backend throught this transport
func (t *UDPTransport) TransferData(payload []string) error {

	size := len(payload)
	if size == 0 {
		return ErrInvalidPayloadSize
	}

	for _, p := range payload {

		err := t.udpNetworkConn.transferData(p)
		if err != nil {
			return err
		}
	}

	return nil
}

// DataChannel - send a new point
func (t *UDPTransport) DataChannel(item interface{}) {

	t.core.dataChannel(item)
}

// MatchType - checks if this transport implementation matches the given type
func (t *UDPTransport) MatchType(tt transportType) bool {

	return tt == typeUDP
}

// Start - starts this transport
func (t *UDPTransport) Start() error {

	return t.core.Start()
}

// Close - closes this transport
func (t *UDPTransport) Close() {

	t.core.Close()
	t.udpNetworkConn.closeConnection()
}
