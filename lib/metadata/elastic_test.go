package metadata

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/uol/gobol/saw"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/tsstats"
)

func TestElasticBackend(t *testing.T) {
	elasticAddress := os.Getenv("ELASTIC_IP")
	if len(elasticAddress) <= 0 {
		t.SkipNow()
	}

	logger, _ := saw.New("DEBUG", "QA")
	if assert.NotNil(t, logger) {
		return
	}
	gstats, err := snitch.New(logger, snitch.Settings{
		Address:  "localhost",
		Interval: "@every 1m",
		KSID:     "macstest",
		Port:     "4243",
		Protocol: "udp",
		Runtime:  true,
		Tags: map[string]string{
			"service": "mycenae-dev-test",
		},
	})
	if !assert.NotNil(t, gstats) || !assert.NoError(t, err) {
		return
	}

	stats, err := tsstats.New(logger, gstats, "* * * * *")
	if !assert.NotNil(t, stats) || !assert.NoError(t, err) {
		return
	}

	backend, err := newElasticBackend(logger, nil, rubber.Settings{
		Seed:    fmt.Sprintf("%s:%d", elasticAddress, 9200),
		Limit:   100,
		Timeout: time.Minute,
		Type:    rubber.ConfigWeightedBackend,
	})

	if assert.NotNil(t, backend) && assert.NoError(t, err) {
		genericMetadataBackendTest(t, backend, logger)
	}
}
