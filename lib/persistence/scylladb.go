package persistence

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
)

type scyllaPersistence struct {
	session *gocql.Session
	logger  *logrus.Logger
	stats   *tsstats.StatsTS

	ksMngr string
}

func newScyllaPersistence(
	session *gocql.Session,
	logger *logrus.Logger,
	stats *tsstats.StatsTS,
) (Backend, error) {
	return &scyllaPersistence{
		session: session,
		logger:  logger,
		stats:   stats,
	}, nil
}

func (backend *scyllaPersistence) CreateKeyspace(
	name, datacenter, contact string,
	ttl time.Duration,
) gobol.Error {
	return newUnimplementedMethod("CreateKeyspace", "scyllaPersistence")
}

func (backend *scyllaPersistence) DeleteKeyspace(id string) gobol.Error {
	return newUnimplementedMethod("DeleteKeyspace", "scyllaPersistence")
}

func (backend *scyllaPersistence) ListKeyspaces() ([]Keyspace, gobol.Error) {
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
				"scyllaPersistence",
			)
		}

		backend.statsQueryError(backend.ksMngr, "ts_keyspace", "select")
		return []Keyspace{}, errPersist(
			"ListKeyspaces",
			"scyllaPersistence",
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
