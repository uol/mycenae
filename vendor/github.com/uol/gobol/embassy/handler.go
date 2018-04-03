package embassy

import (
	"go.uber.org/zap"	
)

// Consul - Object used to construct an
// consul Election and Sevice based on consul tool
type Consul struct {
	Service  *Service
	Election *Election
	KV       *MKV
}

// Settings aggregates Service and Election settings
type Settings struct {
	Consul   *ConsulSettings   `json:"consul"`
	Election *ElectionSettings `json:"election"`
	Service  *ServiceSettings  `json:"service"`
	KV       *KVSettings       `json:"kv"`
}

// New returns an ConsulHandler
func New(settings Settings, log *zap.Logger) (*Consul, error) {

	err := NewConnection(*settings.Consul)
	if err != nil {
		return nil, err
	}

	s, err := NewService(log, *settings.Service)
	if err != nil {
		return nil, err
	}

	e, err := NewElection(log, *settings.Election)
	if err != nil {
		return nil, err
	}

	k, err := NewKV(log, *settings.KV)

	ch := &Consul{
		Service:  s,
		Election: e,
		KV:       k,
	}

	return ch, nil
}
