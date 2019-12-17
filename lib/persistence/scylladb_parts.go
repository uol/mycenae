package persistence

import (
	"fmt"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
)

func (backend *scylladb) addKeyspaceMetadata(ks Keyspace) gobol.Error {
	var (
		start = time.Now()
		query = fmt.Sprintf(formatAddKeyspace, backend.ksMngr)
	)
	if err := backend.session.Query(
		query,
		ks.Name,
		ks.Contact,
		ks.DC,
		ks.Replication,
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
		ks.Name, ks.DC, ks.Replication,
	)
	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(ks.Name, constants.StringsEmpty, "create")
		return errPersist("createKeyspace", "scylladb", err)
	}
	return nil
}

func (backend *scylladb) createTable(keySet, valueColumnType, tableName, functionName string, ttl int) gobol.Error {

	tableTTL := uint64(ttl) * 86400

	query := fmt.Sprintf(
		formatCreateTable,
		keySet,
		tableName,
		valueColumnType,
		tableTTL,
	)

	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(keySet, constants.StringsEmpty, "create")
		return errPersist(functionName, "scylladb", err)
	}

	return nil
}

func (backend *scylladb) createNumericTable(ks Keyspace) gobol.Error {
	return backend.createTable(ks.Name, "double", "ts_number_stamp", "createNumericTable", ks.TTL)
}

func (backend *scylladb) createTextTable(ks Keyspace) gobol.Error {
	return backend.createTable(ks.Name, "text", "ts_text_stamp", "createTextTable", ks.TTL)
}

func (backend *scylladb) setPermissions(ks Keyspace) gobol.Error {
	if len(backend.grantUsername) <= 0 {
		return nil
	}

	for _, format := range formatGrants {
		query := fmt.Sprintf(format, ks.Name, backend.grantUsername)
		if err := backend.session.Query(query).Exec(); err != nil {
			backend.statsQueryError(ks.Name, constants.StringsEmpty, "create")
			return errPersist("setPermissions", "scylladb", err)
		}
	}
	return nil
}
