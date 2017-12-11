package metadata

import (
	"bytes"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
	"github.com/uol/mycenae/lib/tsstats"
)

type elasticBackend struct {
	client *rubber.Elastic
	stats  *tsstats.StatsTS
	logger *logrus.Logger
}

func newElasticBackend(
	logger *logrus.Logger, stats *tsstats.StatsTS, settings rubber.Settings,
) (Backend, error) {
	client, err := rubber.New(logger, settings)
	if err != nil {
		return nil, err
	}

	backend := &elasticBackend{
		client: client,
		stats:  stats,
		logger: logger,
	}
	return backend, nil
}

func (backend *elasticBackend) CreateIndex(name string) gobol.Error {
	body := bytes.NewBufferString(indexMapping)
	start := time.Now()
	_, err := backend.client.CreateIndex(name, body)
	if err != nil {
		backend.statsIndexError(name, "elasticBackend", "post")
		return newPersistenceError("CreateIndex", "elasticBackend", err)
	}

	backend.statsIndex(name, "elasticBackend", "post", time.Since(start))
	return nil
}

func (backend *elasticBackend) DeleteIndex(name string) gobol.Error {
	start := time.Now()
	_, err := backend.client.DeleteIndex(name)
	if err != nil {
		backend.statsIndexError(name, "elasticBackend", "delete")
		return newPersistenceError("DeleteIndex", "elasticBackend", err)
	}
	backend.statsIndex(name, "", "delete", time.Since(start))
	return nil
}
