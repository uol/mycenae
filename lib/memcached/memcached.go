package memcached

import (
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
)

// Manages the memcached operations
// @author rnojiri

// Configuration - memcached configuration
type Configuration struct {
	Pool         []string
	MaxIdleConns int
	Timeout      int
}

// Memcached - main struct
type Memcached struct {
	client *memcache.Client
}

// New - initializes
func New(s *tsstats.StatsTS, c *Configuration) (*Memcached, gobol.Error) {

	stats = s

	mc := &Memcached{
		client: memcache.New(c.Pool...),
	}

	mc.client.MaxIdleConns = c.MaxIdleConns
	mc.client.Timeout = time.Duration(c.Timeout) * time.Millisecond

	return mc, nil
}

// fqn - builds a new fully qualified name using the specified strings
func (mc *Memcached) fqn(namespace string, fqnKeys ...string) (string, gobol.Error) {

	if fqnKeys == nil || len(fqnKeys) == 0 {
		return "", errInternalServerErrorM("fqn", "No ")
	}

	result := namespace + "/"

	for _, item := range fqnKeys {
		result += item
		result += "/"
	}

	return result, nil
}

// Get - returns an object from the cache
func (mc *Memcached) Get(namespace string, fqnKeys ...string) ([]byte, gobol.Error) {

	start := time.Now()

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return nil, err
	}

	item, error := mc.client.Get(fqn)

	if error != nil && error != memcache.ErrCacheMiss {
		return nil, errInternalServerError("get", "error retrieving value from "+fqn, error)
	}

	if item == nil || item.Value == nil {
		statsNotFound(namespace)
		return nil, nil
	}

	statsSuccess("Get", namespace, time.Since(start))

	return item.Value, nil
}

// Put - puts an object in the cache
func (mc *Memcached) Put(value []byte, ttl int32, namespace string, fqnKeys ...string) gobol.Error {

	start := time.Now()

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return err
	}

	item := &memcache.Item{
		Key:        fqn,
		Value:      value,
		Expiration: ttl,
	}

	error := mc.client.Set(item)

	if error != nil {
		statsError("Put", namespace)
		return errInternalServerError("put", "error adding data on "+fqn, err)
	}

	statsSuccess("put", namespace, time.Since(start))

	return nil
}

// Delete - deletes an object from the cache
func (mc *Memcached) Delete(namespace string, fqnKeys ...string) gobol.Error {

	start := time.Now()

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return err
	}

	error := mc.client.Delete(fqn)
	if error != nil && error != memcache.ErrCacheMiss {
		statsError("delete", namespace)
		return errInternalServerError("delete", "error removing data on "+fqn, error)
	}

	statsSuccess("delete", namespace, time.Since(start))

	return nil
}
