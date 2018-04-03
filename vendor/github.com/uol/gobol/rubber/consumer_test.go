package rubber

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
)

var integration = (os.Getenv("INTEGRATION") == "true")
var master = os.Getenv("MASTER")

func TestMain(m *testing.M) {
	if integration {
		os.Exit(m.Run())
	}
}

func createTestConsumer() *consumer {
	logger := zap.NewNop()

	return &consumer{
		server: fmt.Sprintf("%s:9200", master),
		index:  "index",

		client: http.DefaultClient,
		logger: logger,

		input:    make(chan *esRequest),
		shutdown: make(chan bool),

		maxRetries:   10,
		errorTimeout: 3 * time.Second,
	}
}

func TestConsumer(t *testing.T) {
	c := createTestConsumer()
	status, message, err := c.Request(c.server, "PUT", path.Join(c.index), nil)
	assert.NoError(t, err)
	assert.Equal(t, status, http.StatusOK)
	assert.Equal(t, "{\"acknowledged\":true}", string(message))
}
