package memcached

import (
	"fmt"
	"time"

	"github.com/rainycape/memcache"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/tsstats"
)

// Manages the memcached operations
// @author rnojiri

const (
	cGet    = "get"
	cPut    = "put"
	cDelete = "delete"
	cBar    = "/"
)

// Configuration - memcached configuration
type Configuration struct {
	Pool         []string
	MaxIdleConns int
	Timeout      string
}

// Memcached - main struct
type Memcached struct {
	client *memcache.Client
}

// New - initializes
func New(s *tsstats.StatsTS, c *Configuration) (*Memcached, error) {

	stats = s

	timeoutDuration, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return nil, err
	}

	client, err := memcache.New(c.Pool...)
	if err != nil {
		return nil, err
	}

	mc := &Memcached{
		client: client,
	}

	mc.client.SetMaxIdleConnsPerAddr(c.MaxIdleConns)
	mc.client.SetTimeout(timeoutDuration)

	return mc, nil
}

// fqn - builds a new fully qualified name using the specified strings
func (mc *Memcached) fqn(namespace string, fqnKeys ...string) (string, error) {

	if fqnKeys == nil || len(fqnKeys) == 0 {
		return constants.StringsEmpty, fmt.Errorf("no fqn composition keys found")
	}

	result := namespace + cBar

	for _, item := range fqnKeys {
		result += item
		result += cBar
	}

	return result, nil
}

// Get - returns an object from the cache
func (mc *Memcached) Get(namespace string, fqnKeys ...string) ([]byte, error) {

	start := time.Now()

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return nil, err
	}

	item, err := mc.client.Get(fqn)
	if err != nil && err != memcache.ErrCacheMiss {
		return nil, err
	}

	if item == nil || item.Value == nil {
		statsNotFound(namespace)
		return nil, nil
	}

	statsSuccess(cGet, namespace, time.Since(start))

	return item.Value, nil
}

// Put - puts an object in the cache
func (mc *Memcached) Put(value []byte, ttl int32, namespace string, fqnKeys ...string) error {

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

	err = mc.client.Set(item)
	if err != nil {
		statsError(cPut, namespace)
		return err
	}

	statsSuccess(cPut, namespace, time.Since(start))

	return nil
}

// Delete - deletes an object from the cache
func (mc *Memcached) Delete(namespace string, fqnKeys ...string) error {

	start := time.Now()

	fqn, err := mc.fqn(namespace, fqnKeys...)

	if err != nil {
		return err
	}

	error := mc.client.Delete(fqn)
	if error != nil && error != memcache.ErrCacheMiss {
		statsError(cDelete, namespace)
		return err
	}

	statsSuccess(cDelete, namespace, time.Since(start))

	return nil
}
