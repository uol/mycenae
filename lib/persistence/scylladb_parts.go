package persistence

import (
	"fmt"
	"time"

	"github.com/uol/gobol"
)

func (backend *scylladb) addKeyspaceMetadata(ks Keyspace) gobol.Error {
	var (
		start = time.Now()
		query = fmt.Sprintf(formatAddKeyspace, backend.ksMngr)
	)
	if err := backend.session.Query(
		query,
		ks.ID,
		ks.Name,
		ks.Contact,
		ks.DC,
		ks.TTL,
	).Exec(); err != nil {
		backend.statsQueryError(backend.ksMngr, "ts_keyspace", "insert")
		return errPersist("addKeyspaceMetadata", "scylladb", err)
	}

	backend.statsQuery(backend.ksMngr, "ts_keyspace", "insert",
		time.Since(start),
	)
	return nil
}

func (backend *scylladb) createKeyspace(ks Keyspace) gobol.Error {
	query := fmt.Sprintf(
		formatCreateKeyspace,
		ks.ID, ks.DC, ks.TTL,
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
		ks.TTL,
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
			ks.TTL,
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
