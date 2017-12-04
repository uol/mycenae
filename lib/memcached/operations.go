package memcached

import (
	"time"
	"github.com/uol/gobol"
	"github.com/boltdb/bolt"
	"github.com/bradfitz/gomemcache/memcache"
)

func newBolt(path string) (*persistence, gobol.Error) {

	var err error

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, errPersist("New", err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, errPersist("New", err)
	}
	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists([]byte("keyspace")); err != nil {
		return nil, errPersist("New", err)
	}

	if _, err := tx.CreateBucketIfNotExists([]byte("number")); err != nil {
		return nil, errPersist("New", err)
	}

	if _, err := tx.CreateBucketIfNotExists([]byte("text")); err != nil {
		return nil, errPersist("New", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, errPersist("New", err)
	}

	return &persistence{
		db: db,
	}, nil
}

type persistence struct {
	db *bolt.DB
}

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

	item, error := mc.client.Get(fqn);

	if error != nil {
		return nil, errInternalServerError("Get", "Error retrieving value from " + fqn, error)
	}

	if item == nil {
		statsNotFound(namespace)
		return nil, nil
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
		Key: fqn,
		Value: value,
		Expiration: mc.TTL,
	}

	error := mc.client.Set(item)

	if error != nil {
		statsError("Put", namespace)
		return errPersist("Put", err)
	}

	err = tx.Commit()
	if err != nil {
		statsError("put", namespace)
		return errPersist("Put", err)
	}

	statsSuccess("put", namespace, time.Since(start))
	return nil
}

func (mc *Memcached) Delete(buckName, key string) gobol.Error {
	start := time.Now()
	tx, err := persist.db.Begin(true)
	if err != nil {
		statsError("begin", buckName)
		return errPersist("Delete", err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket(buckName)
	if err := bucket.Delete(key); err != nil {
		statsError("delete", buckName)
		return errPersist("delete", err)
	}

	err = tx.Commit()
	if err != nil {
		statsError("delete", buckName)
		return errPersist("delete", err)
	}

	statsSuccess("delete", buckName, time.Since(start))
	return nil
}