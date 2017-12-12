package persistence

import (
	"fmt"

	"github.com/uol/gobol"
)

func (backend *scylladb) createKeyspace(ks Keyspace) gobol.Error {
	query := fmt.Sprintf(
		formatCreateKeyspace,
		ks.ID, ks.DC, backend.ttl,
	)
	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(ks.ID, "", "create")
		return errPersist("createKeyspace", "scylladb", err)
	}
	return nil
}

func (backend *scylladb) createNumericTable(ks Keyspace) gobol.Error {
	query := fmt.Sprintf(
		formatCreateNumericTable,
		ks.ID,
		backend.compaction,
		backend.ttl,
	)

	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(ks.ID, "", "create")
		return errPersist("createNumericTable", "scylladb", err)
	}
	return nil
}

func (backend *scylladb) createTextTable(ks Keyspace) gobol.Error {
	if err := backend.session.Query(
		fmt.Sprintf(
			formatCreateTextTable,
			ks.ID,
			backend.compaction,
			backend.ttl,
		),
	).Exec(); err != nil {
		backend.statsQueryError(ks.ID, "", "create")
		return errPersist("createTextTable", "scylladb", err)
	}
	return nil
}

func (backend *scylladb) setPermissions(ks Keyspace) gobol.Error {
	return nil
}
