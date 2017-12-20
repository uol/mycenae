package persistence

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
)

type scylladb struct {
	session *gocql.Session
	logger  *logrus.Logger
	stats   *tsstats.StatsTS

	ksMngr        string
	compaction    string
	grantUsername string
}

func newScyllaPersistence(
	ksAdmin string,
	grantUsername string,
	session *gocql.Session,
	logger *logrus.Logger,
	stats *tsstats.StatsTS,
) (Backend, error) {
	return &scylladb{
		session: session,
		logger:  logger,
		stats:   stats,

		ksMngr:        ksAdmin,
		grantUsername: grantUsername,
		compaction:    "SizeTieredCompactionStrategy",
	}, nil
}

func (backend *scylladb) CreateKeyspace(
	ksid, name, datacenter, contact string,
	ttl int,
) gobol.Error {
	keyspace := Keyspace{
		ID:      ksid,
		Name:    name,
		DC:      datacenter,
		Contact: contact,
		TTL:     ttl,
	}

	if _, found, err := backend.GetKeyspace(ksid); err != nil {
		return err
	} else if found {
		return errConflict("CreateKeyspace", "scylladb",
			fmt.Sprintf("Keyspace `%s` already exists", ksid),
		)
	}

	if _, found, err := backend.GetKeyspaceByName(name); err != nil {
		return err
	} else if found {
		return errConflict("CreateKeyspace", "scylladb",
			fmt.Sprintf("Keyspace `%s` already exists", ksid),
		)
	}

	// Timing for this management part is executed separately
	if err := backend.addKeyspaceMetadata(keyspace); err != nil {
		return err
	}

	start := time.Now()
	if err := backend.createKeyspace(keyspace); err != nil {
		return err
	}
	if err := backend.createNumericTable(keyspace); err != nil {
		return err
	}
	if err := backend.createTextTable(keyspace); err != nil {
		return err
	}
	if err := backend.setPermissions(keyspace); err != nil {
		return err
	}

	backend.statsQuery(keyspace.ID, "", "create", time.Since(start))
	return nil
}

func (backend *scylladb) DeleteKeyspace(id string) gobol.Error {
	start := time.Now()
	query := fmt.Sprintf(formatDeleteKeyspace, id)
	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(id, "", "drop")
		return errPersist("DeleteKeyspace", "scylladb", err)
	}

	backend.statsQuery(id, "", "drop", time.Since(start))
	return nil
}

func (backend *scylladb) ListKeyspaces() ([]Keyspace, gobol.Error) {
	query := `SELECT key, name, contact, datacenter, ks_ttl FROM %s.ts_keyspace`
	start := time.Now()
	iter := backend.session.Query(fmt.Sprintf(query, backend.ksMngr)).Iter()

	var (
		current   Keyspace
		keyspaces []Keyspace
	)
	for iter.Scan(
		&current.ID,
		&current.Name,
		&current.Contact,
		&current.DC,
		&current.TTL,
	) {
		if current.ID != backend.ksMngr {
			keyspaces = append(keyspaces, current)
		}
	}
	if err := iter.Close(); err != nil {
		if err == gocql.ErrNotFound {
			backend.statsQuery(
				backend.ksMngr,
				"ts_keyspace",
				"select",
				time.Since(start),
			)
			return []Keyspace{}, errNoContent(
				"ListKeyspaces",
				"scylladb",
			)
		}

		backend.statsQueryError(backend.ksMngr, "ts_keyspace", "select")
		return []Keyspace{}, errPersist(
			"ListKeyspaces",
			"scylladb",
			err,
		)
	}

	backend.statsQuery(
		backend.ksMngr,
		"ts_keyspace",
		"select",
		time.Since(start),
	)
	return keyspaces, nil
}

func (backend *scylladb) GetKeyspace(id string) (Keyspace, bool, gobol.Error) {
	var (
		query = fmt.Sprintf(formatGetKeyspace, backend.ksMngr)
		ks    = Keyspace{ID: id}
	)
	if err := backend.session.Query(query, id).Scan(
		&ks.Name, &ks.Contact, &ks.DC, &ks.TTL,
	); err == gocql.ErrNotFound {
		return Keyspace{}, false, nil
	} else if err != nil {
		return Keyspace{}, false, errPersist("GetKeyspace", "scylladb", err)
	}
	return ks, true, nil
}

func (backend *scylladb) GetKeyspaceByName(
	name string,
) (Keyspace, bool, gobol.Error) {
	keyspaces, err := backend.ListKeyspaces()
	if err != nil {
		return Keyspace{}, false, err
	}
	for _, keyspace := range keyspaces {
		if keyspace.Name == name {
			return keyspace, true, nil
		}
	}
	return Keyspace{}, false, nil
}

func (backend *scylladb) UpdateKeyspace(
	ksid, name, contact string,
) gobol.Error {
	start := time.Now()
	query := fmt.Sprintf(formatUpdateKeyspace, backend.ksMngr)

	if _, found, err := backend.GetKeyspaceByName(name); err != nil {
		return err
	} else if found {
		return errConflict(
			"UpdateKeyspace", "scylladb",
			fmt.Sprintf("Keyspace already exist: %s", name),
		)
	}

	if err := backend.session.Query(
		query, name, contact, ksid,
	).Exec(); err != nil {
		backend.statsQueryError(backend.ksMngr, "ts_keyspace", "update")
		return errPersist("UpdateKeyspace", "scylladb", err)
	}

	backend.statsQuery(
		backend.ksMngr,
		"ts_keyspace",
		"update",
		time.Since(start),
	)
	return nil
}
func (backend *scylladb) ListDatacenters() ([]string, gobol.Error) {
	var (
		datacenter  string
		datacenters []string
	)
	query := fmt.Sprintf(formatListDatacenters, backend.ksMngr)
	iter := backend.session.Query(query).Iter()
	for iter.Scan(&datacenter) {
		datacenters = append(datacenters, datacenter)
	}
	if err := iter.Close(); err != nil {
		return nil, errPersist("ListDatacenters", "scylladb", err)
	}
	if len(datacenters) <= 0 {
		return datacenters, errNoContent("ListDatacenters", "scylladb")
	}
	return datacenters, nil
}
