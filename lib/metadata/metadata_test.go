package metadata

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func genericMetadataBackendTest(
	t *testing.T,
	backend Backend,
	logger *logrus.Logger,
) {
	var (
		unique = strings.Replace(uuid.New(), "-", "", -1)

		name = fmt.Sprintf("index-%s", unique)
	)
	if !assert.NotNil(t, backend, "There should be a backend to test") {
		return
	}

	meta := &Storage{
		Backend: backend,
		logger:  logger,
	}

	err := meta.CreateIndex(name)
	assert.NoError(t, err)

	err = meta.DeleteIndex(name)
	assert.NoError(t, err)
}
