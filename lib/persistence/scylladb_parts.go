package persistence

import (
	"fmt"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
)

const funcAddKeyspaceMetadata string = "addKeyspaceMetadata"

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
		backend.statsQueryError(funcAddKeyspaceMetadata, backend.ksMngr, constants.CRUDOperationInsert)
		return errPersist(funcAddKeyspaceMetadata, structName, err)
	}

	backend.statsQuery(funcAddKeyspaceMetadata, backend.ksMngr, constants.CRUDOperationInsert, time.Since(start))

	return nil
}

const funcPrivateCreateKeyspace string = "createKeyspace"

func (backend *scylladb) createKeyspace(ks Keyspace) gobol.Error {
	query := fmt.Sprintf(
		formatCreateKeyspace,
		ks.Name, ks.DC, ks.Replication,
	)

	start := time.Now()

	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(funcPrivateCreateKeyspace, ks.Name, constants.CRUDOperationCreate)
		return errPersist(funcPrivateCreateKeyspace, structName, err)
	}

	backend.statsQuery(funcPrivateCreateKeyspace, ks.Name, constants.CRUDOperationCreate, time.Since(start))

	return nil
}

func (backend *scylladb) createTable(keyset, valueColumnType, tableName, clusteringOrder, functionName string, ttl int) gobol.Error {

	tableTTL := uint64(ttl) * 86400

	query := fmt.Sprintf(
		formatCreateTable,
		keyset,
		tableName,
		valueColumnType,
		clusteringOrder,
		tableTTL,
	)

	start := time.Now()

	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(functionName, keyset, constants.CRUDOperationCreate)
		return errPersist(functionName, structName, err)
	}

	backend.statsQuery(functionName, keyset, constants.CRUDOperationCreate, time.Since(start))

	return nil
}

func (backend *scylladb) createNumericTable(ks Keyspace) gobol.Error {
	return backend.createTable(ks.Name, "double", "ts_number_stamp", backend.clusteringOrder, "createNumericTable", ks.TTL)
}

func (backend *scylladb) createTextTable(ks Keyspace) gobol.Error {
	return backend.createTable(ks.Name, "text", "ts_text_stamp", backend.clusteringOrder, "createTextTable", ks.TTL)
}

func (backend *scylladb) setPermissions(ks Keyspace) gobol.Error {
	if len(backend.grantUsername) <= 0 {
		return nil
	}

	for _, format := range formatGrants {
		query := fmt.Sprintf(format, ks.Name, backend.grantUsername)
		if err := backend.session.Query(query).Exec(); err != nil {
			backend.statsQueryError(ks.Name, constants.StringsEmpty, "create")
			return errPersist("setPermissions", structName, err)
		}
	}
	return nil
}
