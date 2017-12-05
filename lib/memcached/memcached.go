package memcached

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/tsstats"
	"net/http"
)

type Configuration struct {
	Pool []string
	TTL  int32
}

type Memcached struct {
	keyspace *keyspace.Keyspace
	client   *memcache.Client
	TTL      int32
}

func New(s *tsstats.StatsTS, ks *keyspace.Keyspace, c *Configuration) (*Memcached, gobol.Error) {

	stats = s

	mc := &Memcached{
		keyspace: ks,
		client:   memcache.New(c.Pool...),
		TTL:      c.TTL,
	}

	err := mc.Put("test", "test", []byte("test"))

	if err != nil {
		return nil, errInternalServerError("new", "no connection to memcached", err)
	}

	return mc, nil
}

func (mc *Memcached) GetKeyspace(key string) (string, bool, gobol.Error) {

	v, gerr := mc.Get("keyspace", key)
	if gerr != nil {
		return "", false, gerr
	}

	if v != nil {
		return string(v), true, nil
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

	gerr = mc.Put("keyspace", key, []byte(value))
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

	v, gerr := mc.Get(bucket, key)
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

	gerr = mc.Put(bucket, key, []byte{})
	if gerr != nil {
		return false, gerr
	}

	return true, nil
}
