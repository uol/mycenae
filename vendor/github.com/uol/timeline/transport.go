package timeline

import (
	"fmt"
	"time"

	"github.com/uol/funks"
	"github.com/uol/logh"
	"github.com/uol/timeline/buffer"
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
	DataChannel(item interface{})

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

	// Serialize - renders the text using the configured serializer
	Serialize(item interface{}) (string, error)

	// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point
	DataChannelItemToFlattenerPoint(configuration *DataTransformerConf, item interface{}, operation FlatOperation) (Hashable, error)

	// FlattenerPointToDataChannelItem - converts the flattened point to the data channel item
	FlattenerPointToDataChannelItem(item *FlattenerPoint) (interface{}, error)

	// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
	DataChannelItemToAccumulatedData(configuration *DataTransformerConf, item interface{}, calculateHash bool) (Hashable, error)

	// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
	AccumulatedDataToDataChannelItem(item *AccumulatedData) (interface{}, error)
}

// Hashable - a struct with hash function
type Hashable interface {

	// GetHash - return this instance hash
	GetHash() string
}

// transportCore - implements a default transport behaviour
type transportCore struct {
	transport            Transport
	batchSendInterval    time.Duration
	pointBuffer          *buffer.Buffer
	loggers              *logh.ContextualLogger
	started              bool
	defaultConfiguration *DefaultTransportConfiguration
}

// DefaultTransportConfiguration - the default fields used by the transport configuration
type DefaultTransportConfiguration struct {
	TransportBufferSize  int
	BatchSendInterval    funks.Duration
	RequestTimeout       funks.Duration
	SerializerBufferSize int
	DebugInput           bool
	DebugOutput          bool
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

	if t.started {
		return nil
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg("starting transport...")
	}

	t.pointBuffer = buffer.New()
	t.started = true

	go t.transferDataLoop()

	return nil
}

// transferDataLoop - transfers the data to the backend throught this transport
func (t *transportCore) transferDataLoop() {

	if logh.InfoEnabled {
		t.loggers.Info().Msg("initializing transfer data loop...")
	}

	for {
		<-time.After(t.batchSendInterval)

		t.releaseBuffer()
	}
}

// releaseBuffer - releases the point buffer
func (t *transportCore) releaseBuffer() {

	numPoints := t.pointBuffer.GetSize()

	if numPoints == 0 {
		if logh.DebugEnabled {
			t.loggers.Debug().Msg("buffer is empty, no data will be send")
		}
		return
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg(fmt.Sprintf("sending a batch of %d points...", numPoints))
	}

	points := t.pointBuffer.GetAll()
	end := 0

	for i := 0; end != numPoints; i++ {

		var batchBuffer []interface{}

		if numPoints <= t.defaultConfiguration.TransportBufferSize {
			batchBuffer = points
			end = numPoints
		} else {
			start := i * t.defaultConfiguration.TransportBufferSize
			end = start + t.defaultConfiguration.TransportBufferSize

			if end > numPoints {
				end = numPoints
			}

			batchBuffer = points[start:end]
		}

		err := t.transport.TransferData(batchBuffer)
		if err != nil {
			if logh.ErrorEnabled {
				t.loggers.Error().Err(err).Msg("error transferring data")
			}
		} else {
			if logh.InfoEnabled {
				t.loggers.Info().Msgf("batch of %d points were sent!", numPoints)
			}
		}
	}

	return
}

// Close - closes the transport
func (t *transportCore) Close() {

	if logh.InfoEnabled {
		t.loggers.Info().Msg("closing...")
	}

	t.pointBuffer.Release()
	t.started = false
}

// debugInput - print the incoming points if enabled
func (t *transportCore) debugInput(array []interface{}) {

	if t.defaultConfiguration.DebugInput && logh.DebugEnabled {

		for _, item := range array {

			t.loggers.Debug().Str("point", "input").Msgf("%+v", item)
		}
	}
}

// debugOutput - print the outcoming points if enabled
func (t *transportCore) debugOutput(serialized string) {

	if t.defaultConfiguration.DebugOutput && logh.DebugEnabled {

		t.loggers.Debug().Str("point", "output").Msgf("--content-start--\n%s\n--content-end--\n", serialized)
	}
}
