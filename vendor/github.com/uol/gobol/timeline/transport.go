package timeline

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
* The transport interface to be implemented.
* @author rnojiri
**/

type transportType uint8

const (
	typeHTTP transportType = 0
	typeOpenTSDB
)

// Transport - the implementation type to send a event
type Transport interface {

	// Send - send a new point
	DataChannel() chan<- interface{}

	// ConfigureBackend - configures the backend
	ConfigureBackend(backend *Backend) error

	// TransferData - transfers the data using this specific implementation
	TransferData(dataList []interface{}) error

	// Start - starts this transport
	Start() error

	// Close - closes this transport
	Close()

	// MatchType - checks if this transport implementation matches the given type
	MatchType(tt transportType) bool

	// DataChannelItemToFlattenedPoint - converts the data channel item to the flattened point one
	DataChannelItemToFlattenedPoint(operation FlatOperation, item interface{}) (*FlattenerPoint, error)

	// FlattenedPointToDataChannelItem - converts the flattened point to the data channel item one
	FlattenedPointToDataChannelItem(point *FlattenerPoint) (interface{}, error)
}

// transportCore - implements a default transport behaviour
type transportCore struct {
	transport         Transport
	batchSendInterval time.Duration
	pointChannel      chan interface{}
	logger            *zap.Logger
}

// DefaultTransportConfiguration - the default fields used by the transport configuration
type DefaultTransportConfiguration struct {
	TransportBufferSize  int
	BatchSendInterval    time.Duration
	RequestTimeout       time.Duration
	SerializerBufferSize int
}

// Validate - validates the default itens from the configuration
func (c *DefaultTransportConfiguration) Validate() error {

	if c.TransportBufferSize <= 0 {
		return fmt.Errorf("invalid buffer size: %d", c.TransportBufferSize)
	}

	if c.SerializerBufferSize <= 0 {
		return fmt.Errorf("invalid serializer buffer size: %d", c.SerializerBufferSize)
	}

	if c.BatchSendInterval.Seconds() <= 0 {
		return fmt.Errorf("invalid batch send interval: %s", c.BatchSendInterval)
	}

	if c.RequestTimeout.Seconds() <= 0 {
		return fmt.Errorf("invalid request timeout interval: %s", c.RequestTimeout)
	}

	return nil
}

// Start - starts the transport
func (t *transportCore) Start() error {

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "transportCore"),
		zap.String("func", "Start"),
	}

	t.logger.Info("starting transport...", lf...)

	go t.transferDataLoop()

	return nil
}

// transferDataLoop - transfers the data to the backend throught this transport
func (t *transportCore) transferDataLoop() {

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "transportCore"),
		zap.String("func", "transferDataLoop"),
	}

	t.logger.Info("initializing transfer data loop...", lf...)

outterFor:
	for {
		<-time.After(t.batchSendInterval)

		points := []interface{}{}
		numPoints := 0

	innerLoop:
		for {
			select {
			case point, ok := <-t.pointChannel:

				if !ok {
					t.logger.Info("breaking data transfer loop", lf...)
					break outterFor
				}

				points = append(points, point)

			default:
				break innerLoop
			}
		}

		numPoints = len(points)

		if numPoints == 0 {
			t.logger.Info("buffer is empty, no data will be send", lf...)
			continue
		}

		t.logger.Info(fmt.Sprintf("sending a batch of %d points...", numPoints), lf...)

		err := t.transport.TransferData(points)
		if err != nil {
			t.logger.Error(err.Error(), lf...)
		} else {
			t.logger.Info(fmt.Sprintf("batch of %d points were sent!", numPoints), lf...)
		}

	}
}

// Close - closes the transport
func (t *transportCore) Close() {

	lf := []zapcore.Field{
		zap.String("package", "timeline"),
		zap.String("struct", "transportCore"),
		zap.String("func", "Close"),
	}

	t.logger.Info("closing...", lf...)

	close(t.pointChannel)
}
