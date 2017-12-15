package persistence

import "github.com/uol/gobol"

// CreateKeyspace is a wrapper around the Backend in order to create metadata
// with the actual keyspace creation
func (storage *Storage) CreateKeyspace(
	ksid, name, datacenter, contact string, ttl int,
) gobol.Error {
	if err := storage.metadata.CreateIndex(ksid); err != nil {
		return err
	}
	if err := storage.Backend.CreateKeyspace(
		ksid, name, datacenter, contact, ttl,
	); err != nil {
		return err
	}
	return nil
}

// DeleteKeyspace is a wrapper around keyspace deletion to ensure the metadata
// is deleted with the keyspace
func (storage *Storage) DeleteKeyspace(id string) gobol.Error {
	if err := storage.metadata.DeleteIndex(id); err != nil {
		return err
	}
	if err := storage.Backend.DeleteKeyspace(id); err != nil {
		return err
	}
	return nil
}
