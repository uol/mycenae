package memcached

import (
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/keyspace"
	"net/http"
	"github.com/bradfitz/gomemcache/memcache"
)

type Configuration struct {
	Nodes []string
	TTL int32
}

//New creates a a struct that "caches" timeseries keys. It uses memcached as persistence
func New(s *tsstats.StatsTS, ks *keyspace.Keyspace, c *Configuration) (*Memcached) {

	stats = s

	return &Memcached{
		keyspace: ks,
		client: memcache.New(c.Nodes...),
		TTL: c.TTL,
	}
}

//Memcached is responsible for caching timeseries keys from elasticsearch
type Memcached struct {
	keyspace *keyspace.Keyspace
	client *memcache.Client
	TTL int32
}

//GetKeyspace returns a keyspace key, a boolean that tells if the key was found or not and an error.
//If the key isn't in boltdb GetKeyspace tries to fetch the key from cassandra, and if found, puts it in boltdb.
func (mc *Memcached) GetKeyspace(key string) (string, bool, gobol.Error) {

	v, gerr := mc.Get("keyspace", key)
	if gerr != nil {
		return "", false, gerr
	}

	if v != nil {
		return v, true, nil
	}

	ks, found, gerr := mc.keyspace.GetKeyspace(key)
	if gerr != nil {
		if gerr.StatusCode() == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, gerr
	}

	if !found {
		return "", false, nil
	}

	value := "false"

	if ks.TUUID {
		value = "true"
	}

	gerr = mc.persist.Put([]byte("keyspace"), []byte(key), []byte(value))
	if gerr != nil {
		return "", false, gerr
	}

	return value, true, nil
}

func (mc *Memcached) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return mc.getTSID("meta", "number", key, CheckTSID)
}

func (mc *Memcached) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return mc.getTSID("metatext", "text", key, CheckTSID)
}

func (mc *Memcached) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	v, gerr := mc.persist.Get([]byte(bucket), []byte(key))
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

	gerr = mc.persist.Put([]byte(bucket), []byte(key), []byte{})
	if gerr != nil {
		return false, gerr
	}

	return true, nil
}
