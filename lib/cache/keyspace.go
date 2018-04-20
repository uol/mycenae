package cache

import (
	"fmt"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
)

// KeyspaceCache - main structure
type KeyspaceCache struct {
	memcached *memcached.Memcached
	keyspace  *keyspace.Keyspace
}

// NewKeyspaceCache - initializes
func NewKeyspaceCache(mc *memcached.Memcached, ks *keyspace.Keyspace) *KeyspaceCache {

	return &KeyspaceCache{
		memcached: mc,
		keyspace:  ks,
	}
}

// GetTsNumber - checks if the number timeseries exists
func (kc *KeyspaceCache) GetTsNumber(collection, tsid string, CheckTSID func(collection, tsType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return kc.getTSID(collection, "meta", tsid, CheckTSID)
}

// GetTsText - checks if the text timeseries exists
func (kc *KeyspaceCache) GetTsText(collection, tsid string, CheckTSID func(collection, tsType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return kc.getTSID(collection, "metatext", tsid, CheckTSID)
}

// getTSID - generic function to check if number/text timeseries exists
func (kc *KeyspaceCache) getTSID(collection, tsType, tsid string, CheckTSID func(collection, tsType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	bucket := fmt.Sprintf("%s/%s", collection, tsType)

	v, gerr := kc.memcached.Get(bucket, tsid)
	if gerr != nil {
		return false, gerr
	}
	if v != nil {
		return true, nil
	}

	found, gerr := CheckTSID(collection, tsType, tsid)
	if gerr != nil {
		return false, gerr
	}
	if !found {
		return false, nil
	}

	gerr = kc.memcached.Put(bucket, tsid, []byte{})
	if gerr != nil {
		return false, gerr
	}

	return true, nil
}
