package keyset

import (
	"regexp"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/tsstats"
)

// Manages all keyset CRUD and offers some API
// @author: rnojiri

// Manager - the keyset
type Manager struct {
	storage      *metadata.Storage
	stats        *tsstats.StatsTS
	keysetRegexp *regexp.Regexp
}

// New - initializes
func New(storage *metadata.Storage, s *tsstats.StatsTS, keysetRegexp string) *Manager {
	return &Manager{
		storage:      storage,
		stats:        s,
		keysetRegexp: regexp.MustCompile(keysetRegexp),
	}
}

// IsKeysetNameValid - checks if the keyset name is valid
func (ks *Manager) IsKeysetNameValid(keyset string) bool {
	return ks.keysetRegexp.MatchString(keyset)
}

// Create - creates a new index
func (ks *Manager) Create(keyset string) gobol.Error {

	if !ks.IsKeysetNameValid(keyset) {
		return errBadRequest("Create", "invalid keyset name format")
	}

	err := ks.storage.CreateKeyset(keyset)
	if err != nil {
		return errInternalServerError("Create", err)
	}

	return nil
}

// Delete - deletes the keyset
func (ks *Manager) Delete(keyset string) gobol.Error {

	err := ks.storage.DeleteKeyset(keyset)
	if err != nil {
		return errInternalServerError("Delete", err)
	}

	return nil
}

// CheckKeyset - checks if keyset exists
func (ks *Manager) CheckKeyset(keyset string) (bool, gobol.Error) {

	return ks.storage.CheckKeyset(keyset)
}
