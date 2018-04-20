package persistence

import (
	"fmt"

	"github.com/uol/gobol"
)

// DatacenterExists checks whether a given datacenter exists
func (storage *Storage) DatacenterExists(dc string) (bool, gobol.Error) {
	datacenters, err := storage.ListDatacenters()
	if err != nil {
		return false, err
	}
	for _, datacenter := range datacenters {
		if dc == datacenter {
			return true, nil
		}
	}
	return false, nil
}

// CreateKeyspace is a wrapper around the Backend in order to create metadata
// with the actual keyspace creation
func (storage *Storage) CreateKeyspace(
	name, datacenter, contact string,
	replication int, ttl uint8,
) gobol.Error {
	if exists, err := storage.DatacenterExists(datacenter); err != nil {
		return err
	} else if !exists {
		return errNoDatacenter("CreateKeyspace", "Storage",
			fmt.Sprintf(
				"Cannot create because datacenter \"%s\" not exists",
				datacenter,
			),
		)
	}
	if err := storage.Backend.CreateKeyspace(
		name, datacenter, contact, replication, ttl,
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
