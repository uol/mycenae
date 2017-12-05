package memcached

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/uol/gobol"
	"time"
)

func (mc *Memcached) fqn(items ...string) (string, gobol.Error) {

	if items == nil || len(items) == 0 {
		return "", errInternalServerErrorM("fqn", "No ")
	}

	var result string

	for _, item := range items {
		result += item
		result += "/"
	}

	return result, nil
}

func (mc *Memcached) Get(namespace, key string) ([]byte, gobol.Error) {

	start := time.Now()

	fqn, err := mc.fqn(namespace, key)

	if err != nil {
		return nil, err
	}

	item, error := mc.client.Get(fqn)

	if error != nil && error.Error() != "memcache: cache miss" {
		return nil, errInternalServerError("get", "error retrieving value from "+fqn, error)
	}

	if item == nil || item.Value == nil {
		statsNotFound(namespace)
		return nil, nil
	}

	error = mc.client.Touch(fqn, mc.TTL)

	if error != nil {
		return nil, errInternalServerError("touch", "error touching value from "+fqn, error)
	}

	statsSuccess("Get", namespace, time.Since(start))

	return item.Value, nil
}

func (mc *Memcached) Put(namespace, key string, value []byte) gobol.Error {

	start := time.Now()

	fqn, err := mc.fqn(namespace, key)

	if err != nil {
		return err
	}

	item := &memcache.Item{
		Key:        fqn,
		Value:      value,
		Expiration: mc.TTL,
	}

	error := mc.client.Set(item)

	if error != nil {
		statsError("Put", namespace)
		return errInternalServerError("put", "error adding data on "+fqn, err)
	}

	statsSuccess("put", namespace, time.Since(start))

	return nil
}

func (mc *Memcached) Delete(namespace, key string) gobol.Error {

	start := time.Now()

	fqn, err := mc.fqn(namespace, key)

	if err != nil {
		return err
	}

	error := mc.client.Delete(fqn)
	if error != nil {
		statsError("delete", namespace)
		return errInternalServerError("delete", "error removing data on "+fqn, error)
	}

	statsSuccess("delete", namespace, time.Since(start))

	return nil
}
