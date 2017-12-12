package persistence

import (
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func genericPersistenceBackendTest(
	t *testing.T,
	backend Backend,
	logger *logrus.Logger,
) {
	const (
		name       = "testindex"
		datacenter = "dc1"
		contact    = "doe@example.org"
		ttl        = 356 * 24 * 60 * 60
	)
	var (
		unique = GenerateKeyspaceIdentifier()
	)

	storage := &Storage{
		logger:  logger,
		Backend: backend,
	}

	err := storage.CreateKeyspace(unique, name, datacenter, contact, ttl)
	if !assert.NoError(t, err) {
		return
	}
	assert.NoError(t, storage.DeleteKeyspace(unique))
}
