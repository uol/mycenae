package cache

import (
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
)

type KeyspaceCache struct {
	memcached *memcached.Memcached
	keyspace  *keyspace.Keyspace
}

func NewKeyspaceCache(mc *memcached.Memcached, ks *keyspace.Keyspace) *KeyspaceCache {

	return &KeyspaceCache{
		memcached: mc,
		keyspace:  ks,
	}
}

func (kc *KeyspaceCache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return kc.getTSID("meta", "number", key, CheckTSID)
}

func (kc *KeyspaceCache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return kc.getTSID("metatext", "text", key, CheckTSID)
}

func (kc *KeyspaceCache) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	v, gerr := kc.memcached.Get(bucket, key)
	if gerr != nil {
		return false, gerr
	}
	if v != nil {
		return true, nil
	}

	found, gerr := CheckTSID(esType, key)
	if gerr != nil {
		return false, gerr
	}
	if !found {
		return false, nil
	}

	gerr = kc.memcached.Put(bucket, key, []byte{})
	if gerr != nil {
		return false, gerr
	}

	return true, nil
}
