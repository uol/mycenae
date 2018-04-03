package embassy

import (
	"errors"
	"time"

	"github.com/hashicorp/consul/api"

	"go.uber.org/zap"
)

// ErrConsulInvalidPath happens when a given KV path is not present
var ErrConsulInvalidPath = errors.New("Consul invalid path")

// KVSettings is the configuration for the KV module
type KVSettings struct {
	Path string
}

// NewKV returns a MKV object
func NewKV(log *zap.Logger, settings KVSettings) (*MKV, error) {
	if consulClient == nil {
		return nil, errors.New("No agent connection found")
	}

	kv := &MKV{
		log:      log,
		settings: settings,
		client:   consulClient,
		skac:     make(chan struct{}),
		swsc:     make(chan struct{}),
	}
	return kv, nil
}

// MKV is a wrapper around consul's KV API
type MKV struct {
	log      *zap.Logger
	settings KVSettings
	client   *api.Client
	watching bool
	poolInt  time.Duration
	sw       ServiceWatcher
	skac     chan struct{}
	swsc     chan struct{}
}

// Put adds a value to the KV store
func (mkv *MKV) Put(path string, value []byte) error {

	kv := mkv.client.KV()

	p := &api.KVPair{Key: path, Value: value}

	m, err := kv.Put(p, nil)
	if err != nil {
		mkv.log.Error(
			err.Error(),
			zap.String("struct", "MKV"),
			zap.String("func", "Put"),
			zap.Error(err),
		)
		return err
	}
	mkv.log.Info(
		"requestTime",
		zap.String("requestTime", string(m.RequestTime)),
		zap.String("struct", "MKV"),
		zap.String("func", "Put"),
	)

	return nil
}

// Get retrieves a value from consul
func (mkv *MKV) Get(path string) ([]byte, error) {

	kv := mkv.client.KV()

	pair, m, err := kv.Get(path, nil)
	if err != nil {
		mkv.log.Error(
			err.Error(),
			zap.String("struct", "MKV"),
			zap.String("func", "Get"),
			zap.Error(err),
		)
		return nil, err
	}
	mkv.log.Info(
		"requestTime",
		zap.String("requestTime", string(m.RequestTime)),
		zap.String("struct", "MKV"),
		zap.String("func", "Get"),
	)

	if pair == nil {
		return nil, ErrConsulInvalidPath
	}

	return pair.Value, nil
}
