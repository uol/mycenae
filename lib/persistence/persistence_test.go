package persistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func genericPersistenceBackendTest(
	t *testing.T,
	backend Backend,
	logger *zap.Logger,
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
	_ = storage

	err := backend.CreateKeyspace(unique, name, datacenter, contact, ttl)
	if !assert.NoError(t, err) {
		return
	}

	keyspace, found, err := backend.GetKeyspace(unique)
	if assert.NoError(t, err) && assert.True(t, found) {
		assert.Equal(t, name, keyspace.Name)
		assert.Equal(t, datacenter, keyspace.DC)
		assert.Equal(t, contact, keyspace.Contact)
		assert.Equal(t, ttl, keyspace.TTL)
	}

	assert.NoError(t, backend.DeleteKeyspace(unique))

	datacenters, err := backend.ListDatacenters()
	if assert.NoError(t, err) && assert.NotNil(t, datacenters) {
		assert.NotEmpty(t, datacenters)
		if assert.Len(t, datacenters, 1) {
			assert.Equal(t, scyllaTestDatacenter, datacenters[0])
		}
	}
}
