package rubber

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type weightedBackend struct {
	logger    *zap.Logger
	client    *http.Client
	consumers []*consumer
	indices   map[string]chan *esRequest
	indicesL  sync.RWMutex

	seed  string
	limit int

	errorTimeout time.Duration
	maxRetries   uint64

	retriesCounter uint64
}

// CreateWeightedBackend creates a backend that directs requests for a given shard to its server
func CreateWeightedBackend(settings Settings, logger *zap.Logger) (Backend, error) {
	backend := &weightedBackend{
		logger: logger,
		client: &http.Client{
			Timeout: settings.Timeout * time.Second,
		},
		consumers: make([]*consumer, 0),
		indices:   make(map[string]chan *esRequest),
		seed:      settings.Seed,
		limit:     settings.Limit,

		maxRetries:   3,
		errorTimeout: 3 * time.Second,
	}
	if err := backend.start(); err != nil {
		return nil, err
	}
	go backend.updateLoop(time.Minute)
	return backend, nil
}

// NoIndex is the index to be given to the request functions when there is no index
const NoIndex = ""

type shardFormat struct {
	Index   string `json:"index"`
	Shard   string `json:"shard"`
	Prirep  string `json:"prirep"`
	State   string `json:"state"`
	Address string `json:"ip"`
}

func (es *weightedBackend) listIndices() (map[string][]string, error) {
	resp, err := es.client.Get(fmt.Sprintf("http://%s/_cat/shards?format=json", es.seed))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var shards []shardFormat
	if err := json.NewDecoder(resp.Body).Decode(&shards); err != nil {
		return nil, err
	}
	answ := make(map[string][]string)
	for _, shard := range shards {
		if shard.Index != "" && shard.Address != "" {
			answ[shard.Index] = append(answ[shard.Index], shard.Address)
		}
	}
	return answ, nil
}

type nodesFormat struct {
	Address string `json:"ip"`
}

func (es *weightedBackend) listServers() ([]string, error) {
	resp, err := es.client.Get(fmt.Sprintf("http://%s/_cat/nodes?format=json", es.seed))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var nodes []nodesFormat
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}

	var answ []string
	for _, node := range nodes {
		answ = append(answ, node.Address)
	}
	return answ, nil
}

func deleteOldConsumers(consumers []*consumer) error {
	for _, c := range consumers {
		c.close()
	}
	return nil
}

func (es *weightedBackend) addIndex(index string) chan *esRequest {
	es.indicesL.Lock()
	defer es.indicesL.Unlock()

	channel := make(chan *esRequest, es.limit)
	es.indices[index] = channel
	return channel
}

func (es *weightedBackend) getIndex(index string) (chan *esRequest, bool) {
	es.indicesL.RLock()
	defer es.indicesL.RUnlock()

	channel, ok := es.indices[index]
	return channel, ok
}

func (es *weightedBackend) getOrCreateIndex(index string) chan *esRequest {
	channel, ok := es.getIndex(index)
	if ok {
		return channel
	}
	return es.addIndex(index)
}

func (es *weightedBackend) start() error {
	var consumers []*consumer

	indices, err := es.listIndices()
	if err != nil {
		return err
	}
	for index, servers := range indices {
		for _, server := range servers {
			c := &consumer{
				server:   fmt.Sprintf("%s:9200", server),
				index:    index,
				client:   es.client,
				logger:   es.logger,
				input:    es.getOrCreateIndex(index),
				shutdown: make(chan bool),

				maxRetries:   es.maxRetries,
				errorTimeout: es.errorTimeout,
			}
			consumers = append(consumers, c)
			go c.loop()
		}
	}

	servers, err := es.listServers()
	if err != nil {
		return err
	}
	channel := es.getOrCreateIndex(NoIndex)
	for _, server := range servers {
		c := &consumer{
			server:   fmt.Sprintf("%s:9200", server),
			index:    NoIndex,
			client:   es.client,
			logger:   es.logger,
			input:    channel,
			shutdown: make(chan bool),

			maxRetries:   es.maxRetries,
			errorTimeout: es.errorTimeout,
		}
		consumers = append(consumers, c)
		go c.loop()
	}

	oldConsumers := es.consumers
	es.consumers = consumers

	deleteOldConsumers(oldConsumers)
	return nil
}

func (es *weightedBackend) updateLoop(duration time.Duration) {
	for range time.NewTicker(duration).C {
		es.logger.Info("Updating consumers")
		if err := es.start(); err != nil {
			es.logger.Error("", zap.Error(err))
		}
	}
}

func (es *weightedBackend) chose(index string) (chan *esRequest, bool) {
	channel, ok := es.getIndex(index)
	if !ok {
		// Fall back to the no index default course of action
		channel, ok = es.getIndex(NoIndex)
		if !ok {
			// It shouldn't, but who knows?
			return nil, false
		}
	}
	return channel, true
}

func (es *weightedBackend) Request(index, method, path string, body io.Reader) (int, []byte, error) {
	request := esRequest{
		index:  index,
		method: method,
		path:   path,
		body:   body,
		answ:   make(chan esResponse),
	}
	defer close(request.answ)

	channel, ok := es.chose(index)
	if !ok {
		return 0, nil, errors.New("Couldn't choose elasticsearch server")
	}
	if len(channel) >= es.limit {
		return http.StatusTooManyRequests, nil, errors.New("Too many requests on the server")
	}
	channel <- &request
	response := <-request.answ

	atomic.AddUint64(&es.retriesCounter, request.retries)
	return response.status, response.content, response.err
}

func (es *weightedBackend) CountRetries() uint64 { return atomic.SwapUint64(&es.retriesCounter, 0) }
