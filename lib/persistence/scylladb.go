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
	grantUsername string
	devMode       bool
	defaultTTL    uint8
}

func newScyllaPersistence(
	ksAdmin string,
	grantUsername string,
	session *gocql.Session,
	logger *logrus.Logger,
	stats *tsstats.StatsTS,
	devMode bool,
	defaultTTL uint8,
) (Backend, error) {
	return &scylladb{
		session: session,
		logger:  logger,
		stats:   stats,

		ksMngr:        ksAdmin,
		grantUsername: grantUsername,
		devMode:       devMode,
		defaultTTL:    defaultTTL,
	}, nil
}

func (backend *scylladb) CreateKeyspace(
	name, datacenter, contact string,
	replication int, ttl uint8,
) gobol.Error {
	keyspace := Keyspace{
		Name:        name,
		DC:          datacenter,
		Contact:     contact,
		TTL:         ttl,
		Replication: replication,
	}

	if _, found, err := backend.GetKeyspace(name); err != nil {
		return err
	} else if found {
		return errConflict("CreateKeyspace", "scylladb",
			fmt.Sprintf(
				"Cannot create because keyspace \"%s\" already exists",
				name,
			),
		)
	}

	if backend.devMode {
		keyspace.TTL = backend.defaultTTL
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

	backend.statsQuery(keyspace.Name, "", "create", time.Since(start))
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
	query := `SELECT key, contact, datacenter, replication_factor FROM %s.ts_keyspace`
	start := time.Now()
	iter := backend.session.Query(fmt.Sprintf(query, backend.ksMngr)).Iter()

	var (
		current   Keyspace
		keyspaces []Keyspace
	)
	for iter.Scan(
		&current.Name,
		&current.Contact,
		&current.DC,
		&current.Replication,
	) {
		if current.Name != backend.ksMngr {
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
		ks    = Keyspace{Name: id}
	)
	if err := backend.session.Query(query, id).Scan(
		&ks.Name, &ks.Contact, &ks.DC, &ks.Replication,
	); err == gocql.ErrNotFound {
		return Keyspace{}, false, nil
	} else if err != nil {
		return Keyspace{}, false, errPersist("GetKeyspace", "scylladb", err)
	}
	return ks, true, nil
}

func (backend *scylladb) UpdateKeyspace(
	ksid, contact string,
) gobol.Error {
	start := time.Now()
	query := fmt.Sprintf(formatUpdateKeyspace, backend.ksMngr)

	if _, found, err := backend.GetKeyspace(ksid); err != nil {
		return err
	} else if !found {
		return errNotFound("UpdateKeyspace", "scylladb", "")
	}

	if err := backend.session.Query(
		query, contact, ksid,
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
