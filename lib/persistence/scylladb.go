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

	ksMngr     string
	compaction string
}

func newScyllaPersistence(
	ksAdmin string,
	session *gocql.Session,
	logger *logrus.Logger,
	stats *tsstats.StatsTS,
) (Backend, error) {
	return &scylladb{
		session: session,
		logger:  logger,
		stats:   stats,

		ksMngr:     ksAdmin,
		compaction: "SizeTieredCompactionStrategy",
	}, nil
}

func (backend *scylladb) CreateKeyspace(
	ksid, name, datacenter, contact string,
	ttl time.Duration,
) gobol.Error {
	keyspace := Keyspace{
		ID:      ksid,
		Name:    name,
		DC:      datacenter,
		Contact: contact,
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
