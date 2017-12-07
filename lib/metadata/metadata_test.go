package metadata

import (
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func genericMetadataBackendTest(
	t *testing.T,
	backend Backend,
	logger *logrus.Logger,
) {
	if !assert.NotNil(t, backend, "There should be a backend to test") {
		return
	}

	meta := &Storage{
		backend: backend,
		logger:  logger,
	}
	_ = meta

}
