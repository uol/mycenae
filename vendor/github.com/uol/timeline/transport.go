package timeline

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/uol/logh"
	"github.com/uol/timeline/buffer"
)

/**
* The transport interface to be implemented.
* @author rnojiri
**/

type transportType uint8

const (
	typeHTTP     transportType = 1
	typeOpenTSDB transportType = 2
	typeUDP      transportType = 3
)

var (
	// ErrInvalidPayloadSize - raised when the transport receives an invalid payload size
	ErrInvalidPayloadSize error = errors.New("invalid payload size")
)

// Transport - the implementation type to send a event
type Transport interface {

	// Send - send a new point
	DataChannel(item interface{})

	// ConfigureBackend - configures the backend
	ConfigureBackend(backend *Backend) error

	// TransferData - transfers the data using this specific implementation
	TransferData(payload []string) error

	// SerializePayload - serializes a list of generic data
	SerializePayload(dataList []interface{}) (payload []string, err error)

	// Start - starts this transport
	Start() error

	// Close - closes this transport
	Close()

	// MatchType - checks if this transport implementation matches the given type
	MatchType(tt transportType) bool

	// Serialize - renders the text using the configured serializer
	Serialize(item interface{}) (string, error)

	// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point
	DataChannelItemToFlattenerPoint(configuration *DataTransformerConfig, item interface{}, operation FlatOperation) (Hashable, error)

	// FlattenerPointToDataChannelItem - converts the flattened point to the data channel item
	FlattenerPointToDataChannelItem(item *FlattenerPoint) (interface{}, error)

	// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
	DataChannelItemToAccumulatedData(configuration *DataTransformerConfig, item interface{}, calculateHash bool) (Hashable, error)

	// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
	AccumulatedDataToDataChannelItem(item *accumulatedData) (interface{}, error)

	// BuildContextualLogger - build the contextual logger using more info
	BuildContextualLogger(path ...string)
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
	started              uint32
	defaultConfiguration *DefaultTransportConfig
}

// Validate - validates the default itens from the configuration
func (c *DefaultTransportConfig) Validate() error {

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

	if atomic.LoadUint32(&t.started) == 1 {
		return nil
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg("starting transport...")
	}

	t.pointBuffer = buffer.New()
	atomic.StoreUint32(&t.started, 1)

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

		payload, size, err := t.serialize(batchBuffer)
		if err != nil {
			if logh.ErrorEnabled {
				ev := t.loggers.Error()
				if t.defaultConfiguration.PrintStackOnError {
					ev = ev.Caller()
				}
				ev.Err(err).Msg("error serializing data")
			}
			return
		}

		err = t.transport.TransferData(payload)
		if err != nil {
			if logh.ErrorEnabled {
				ev := t.loggers.Error()
				if t.defaultConfiguration.PrintStackOnError {
					ev = ev.Caller()
				}
				ev.Err(err).Msg("error transferring data")
			}
		} else {
			if logh.InfoEnabled {
				byteCount := 0
				for _, p := range payload {
					byteCount += len(p)
				}
				t.loggers.Info().Msgf("batch of %d points were sent! (%d bytes)", size, byteCount)
			}
		}

		<-time.After(t.defaultConfiguration.TimeBetweenBatches.Duration)
	}

	return
}

// serialize - serializes the sent data
func (t *transportCore) serialize(dataList []interface{}) (payload []string, size int, err error) {

	numItems := len(dataList)
	if numItems == 0 {

		if logh.WarnEnabled {
			t.loggers.Warn().Msg("no points to transfer")
		}

		return
	}

	filtered := []interface{}{}

	for i := 0; i < numItems; i++ {

		if dataList[i] == nil {

			if logh.ErrorEnabled {
				ev := t.loggers.Error()
				if t.defaultConfiguration.PrintStackOnError {
					ev = ev.Caller()
				}
				ev.Msgf("null point at buffer index: %d", i)
			}

			continue
		}

		filtered = append(filtered, dataList[i])
	}

	size = len(filtered)

	t.debugInput(dataList)

	payload, err = t.transport.SerializePayload(filtered)
	if err != nil {
		return
	}

	t.debugOutput(payload)

	return
}

// Close - closes the transport
func (t *transportCore) Close() {

	if logh.InfoEnabled {
		t.loggers.Info().Msg("closing...")
	}

	t.pointBuffer.Release()
	atomic.StoreUint32(&t.started, 0)
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
func (t *transportCore) debugOutput(serialized []string) {

	if t.defaultConfiguration.DebugOutput && logh.DebugEnabled {

		for _, s := range serialized {
			t.loggers.Debug().Str("point", "output").Msgf("--content-start--\n%s\n--content-end--\n", s)
		}
	}
}

func (t *transportCore) dataChannel(item interface{}) {

	if item == nil {
		return
	}

	k := reflect.TypeOf(item).Kind()
	if k == reflect.Array || k == reflect.Slice {
		v := reflect.ValueOf(item)
		for i := 0; i < v.Len(); i++ {
			t.pointBuffer.Add(v.Index(i).Interface())
		}
	} else {
		t.pointBuffer.Add(item)
	}
}
