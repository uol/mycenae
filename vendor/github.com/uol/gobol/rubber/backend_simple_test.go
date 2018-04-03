package rubber

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

// This mostly tests compilation
var _ Backend = &singleServerBackend{}

func testSimpleBackend() *singleServerBackend {
	logger := zap.NewNop()
	return &singleServerBackend{
		log:     logger,
		nodes:   []string{fmt.Sprintf("%s:9200", master)},
		timeout: time.Minute,
		client: &http.Client{
			Timeout: time.Minute,
		},
	}
}

func TestSimpleIntegration(t *testing.T) {
	genericBackendTest(t, testSimpleBackend())
}
