package rubber

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	TestSize = 10

	DefaultUpdate = time.Second * 10
)

// This mostly tests compilation
var _ Backend = &weightedBackend{}

func testWeightedBackend(update time.Duration) *weightedBackend {
	logger := zap.NewNop()
	backend := &weightedBackend{
		logger: logger,
		client: &http.Client{
			Timeout: time.Minute,
		},
		consumers: make([]*consumer, 0),
		indices:   make(map[string]chan *esRequest),
		seed:      fmt.Sprintf("%s:9200", master),
		limit:     1024,

		maxRetries:   1,
		errorTimeout: 3 * time.Second,
	}
	backend.start()
	go backend.updateLoop(update)
	return backend
}

func createIndex(index string) error {
	request, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:9200/%s", master, index), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func createIndices() ([]string, error) {
	var indices = []string{esIndex}
	err := createIndex(esIndex)
	if err != nil {
		return nil, err
	}

	for i := 0; i < TestSize; i++ {
		index := uuid.New()
		indices = append(indices, index)
		err := createIndex(index)
		if err != nil {
			return nil, err
		}
	}
	time.Sleep(10 * time.Second)
	return indices, nil
}

func TestFailCall(t *testing.T) {
	backend := testWeightedBackend(time.Minute * 5)
	backend.client.Timeout = time.Second
	es := Create(backend)
	assert.NotNil(t, es)
	time.Sleep(5 * time.Second)

	for index := range backend.consumers {
		backend.consumers[index].server = "192.168.0.1:1700"
	}
	status, _, err := es.Request(NoIndex, "GET", "_cat/nodes", nil)

	assert.Error(t, err)
	assert.NotEqual(t, http.StatusOK, status)
	assert.Equal(t, backend.maxRetries, es.CountRetries())
}

func TestWeightedIntegration(t *testing.T) {
	backend := testWeightedBackend(DefaultUpdate)
	es := Create(backend)
	assert.NotNil(t, es)
	indices, err := createIndices()

	assert.NoError(t, err)
	assert.NoError(t, backend.start())
	assert.NotEmpty(t, backend.consumers)
	assert.NotEmpty(t, backend.indices)

	servers, err := backend.listServers()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(servers))

	for _, index := range indices {
		resp, err := http.DefaultClient.Get(fmt.Sprintf("http://%s:9200/%s", master, index))
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
	}

	for _, index := range indices {
		channel, ok := backend.chose(index)

		assert.True(t, ok)
		for _, c := range backend.consumers {
			if c.index == index {
				assert.Equal(t, c.input, channel)
			}
		}
	}
	genericBackendTest(t, backend)
}

func TestWeightedIndexCreation(t *testing.T) {
	var (
		index = uuid.New()
	)
	backend := testWeightedBackend(DefaultUpdate)
	es := Create(backend)
	assert.NotNil(t, es)

	status, err := es.CreateIndex(index, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
}
