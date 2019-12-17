package logh_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uol/gobol/logh"
)

// TestGlobalConfiguration - tests the global configuration
func TestGlobalConfiguration(t *testing.T) {

	logh.ConfigureGlobalLogger(logh.INFO, logh.CONSOLE)

	assert.True(t, logh.Info() != nil, "expected true")
	assert.False(t, logh.Debug() != nil, "expected false")
	assert.True(t, logh.Warn() != nil, "expected true")
	assert.True(t, logh.Error() != nil, "expected true")
	assert.True(t, logh.Fatal() != nil, "expected true")
	assert.True(t, logh.Panic() != nil, "expected true")
}

// TestContextualLogger - no error expected
func TestContextualLogger(t *testing.T) {

	logh.ConfigureGlobalLogger(logh.ERROR, logh.CONSOLE)

	cl := logh.CreateContextualLogger("context1", "test", "context2", "lalala")

	cl.Info().Msg("hello")
	cl.Info().Msg("world")
}
