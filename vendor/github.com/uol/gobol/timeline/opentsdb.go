package timeline

import (
	"fmt"
	"io"
	"net"
	"time"

	serializer "github.com/uol/serializer/opentsdb"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
* The OpenTSDB transport implementation.
* @author rnojiri
**/

// OpenTSDBTransport - implements the openTSDB transport
type OpenTSDBTransport struct {
	core          transportCore
	configuration *OpenTSDBTransportConfig
	serializer    *serializer.Serializer
	address       *net.TCPAddr
	connection    net.Conn
}

// OpenTSDBTransportConfig - has all openTSDB event manager configurations
type OpenTSDBTransportConfig struct {
	DefaultTransportConfiguration
	MaxReadTimeout      time.Duration
	ReconnectionTimeout time.Duration
}

type rwOp string

const (
	read  rwOp = "read"
	write rwOp = "write"
)

// NewOpenTSDBTransport - creates a new openTSDB event manager
func NewOpenTSDBTransport(configuration *OpenTSDBTransportConfig, logger *zap.Logger) (*OpenTSDBTransport, error) {

	if configuration == nil {
		return nil, fmt.Errorf("null configuration found")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	if configuration.MaxReadTimeout.Seconds() <= 0 {
		return nil, fmt.Errorf("invalid connection maximum read timeout: %s", configuration.MaxReadTimeout)
	}

	if configuration.ReconnectionTimeout.Seconds() <= 0 {
		return nil, fmt.Errorf("invalid connection reconnection timeout: %s", configuration.ReconnectionTimeout)
	}

	s := serializer.New(configuration.SerializerBufferSize)

	t := &OpenTSDBTransport{
		core: transportCore{
			batchSendInterval: configuration.BatchSendInterval,
			pointChannel:      make(chan interface{}, configuration.TransportBufferSize),
			logger:            logger,
		},
		configuration: configuration,
		serializer:    s,
	}

	t.core.transport = t

	return t, nil
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

	t.retryConnect()

	return nil
}

// DataChannel - send a new point
func (t *OpenTSDBTransport) DataChannel() chan<- interface{} {

	return t.core.pointChannel
}

// recover - recovers from panic
func (t *OpenTSDBTransport) recover(lf []zapcore.Field) {

	if r := recover(); r != nil {
		t.core.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
	}
}

// TransferData - transfers the data to the backend throught this transport
func (t *OpenTSDBTransport) TransferData(dataList []interface{}) error {

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

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "OpenTSDBTransport"),
		zap.String("func", "TransferData"),
	}

	defer t.recover(lf)

	for {
		if !t.writePayload(payload, lf) {
			t.closeConnection(lf)
			t.retryConnect()
		} else {
			break
		}
	}

	return nil
}

// writePayload - writes the payload
func (t *OpenTSDBTransport) writePayload(payload string, logFields []zapcore.Field) bool {

	readBuffer := make([]byte, 32)

	err := t.connection.SetReadDeadline(time.Now().Add(t.configuration.MaxReadTimeout))
	if err != nil {
		t.core.logger.Error(fmt.Sprintf("error setting read deadline: %s", err.Error()), logFields...)
		return false
	}

	_, err = t.connection.Read(readBuffer)
	if err != nil {
		if castedErr, ok := err.(net.Error); ok && !castedErr.Timeout() {
			t.logConnectionError(err, read, logFields)
			return false
		}
	}

	err = t.connection.SetWriteDeadline(time.Now().Add(t.configuration.RequestTimeout))
	if err != nil {
		t.core.logger.Error(fmt.Sprintf("error writing on connection: %s", err.Error()), logFields...)
		return false
	}

	_, err = t.connection.Write([]byte(payload))
	if err != nil {
		t.logConnectionError(err, read, logFields)
		return false
	}

	return true
}

// logConnectionError - logs the connection error
func (t *OpenTSDBTransport) logConnectionError(err error, operation rwOp, lf []zapcore.Field) {

	if err == io.EOF {
		t.core.logger.Info(fmt.Sprintf("[%s] connection EOF received, retrying connection...", operation), lf...)
	}

	if castedErr, ok := err.(net.Error); ok && castedErr.Timeout() {
		t.core.logger.Info(fmt.Sprintf("[%s] connection timeout received, retrying connection...", operation), lf...)
	}

	t.core.logger.Error(fmt.Sprintf("[%s] error executing operation on connection: %s", operation, err.Error()), lf...)
}

// closeConnection - closes the active connection
func (t *OpenTSDBTransport) closeConnection(logFields []zapcore.Field) {

	err := t.connection.Close()
	if err != nil {
		t.core.logger.Error(err.Error(), logFields...)
	}

	t.core.logger.Info("connection closed", logFields...)

	t.connection = nil
}

// MatchType - checks if this transport implementation matches the given type
func (t *OpenTSDBTransport) MatchType(tt transportType) bool {

	return tt == typeOpenTSDB
}

// DataChannelItemToFlattenedPoint - converts the data channel item to the flattened point one
func (t *OpenTSDBTransport) DataChannelItemToFlattenedPoint(operation FlatOperation, instance interface{}) (*FlattenerPoint, error) {

	item, ok := instance.(*serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting instance to data channel item")
	}

	hashParameters := []interface{}{}
	hashParameters = append(hashParameters, operation, item.Metric)
	hashParameters = append(hashParameters, item.Tags...)

	if item.Timestamp <= 0 {
		item.Timestamp = time.Now().Unix()
	}

	return &FlattenerPoint{
		value:          item.Value,
		hashParameters: hashParameters,
		flattenerPointData: flattenerPointData{
			operation:       operation,
			timestamp:       item.Timestamp,
			dataChannelItem: *item,
		},
	}, nil
}

// FlattenedPointToDataChannelItem - converts the flattened point to the data channel one
func (t *OpenTSDBTransport) FlattenedPointToDataChannelItem(point *FlattenerPoint) (interface{}, error) {

	item, ok := point.dataChannelItem.(serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting point's data channel item")
	}

	item.Value = point.value

	return item, nil
}

var retryConnectLogFields = []zapcore.Field{
	zap.String("package", "timeline"),
	zap.String("struct", "OpenTSDBTransport"),
	zap.String("func", "retryConnect"),
}

// retryConnect - connects the telnet client
func (t *OpenTSDBTransport) retryConnect() {

	connected := false
	for {
		connected = t.connect()
		if connected {
			break
		}

		<-time.After(t.configuration.ReconnectionTimeout)
	}

	t.core.logger.Info("connected!", retryConnectLogFields...)
}

var connectLogFields = []zapcore.Field{
	zap.String("package", "timeline"),
	zap.String("struct", "OpenTSDBTransport"),
	zap.String("func", "connect"),
}

// connect - connects the telnet client
func (t *OpenTSDBTransport) connect() bool {

	t.core.logger.Info(fmt.Sprintf("connnecting to opentsdb telnet: %s:", t.address.String()), connectLogFields...)

	var err error
	t.connection, err = net.DialTCP("tcp", nil, t.address)
	if err != nil {
		t.core.logger.Error(fmt.Sprintf("error connecting to address: %s", t.address.String()), connectLogFields...)
		return false
	}

	err = t.connection.SetDeadline(time.Time{})
	if err != nil {
		t.core.logger.Error("error setting connection's deadline", connectLogFields...)
		return false
	}

	return true
}

// Start - starts this transport
func (t *OpenTSDBTransport) Start() error {

	return t.core.Start()
}

// Close - closes this transport
func (t *OpenTSDBTransport) Close() {

	t.core.Close()
}
