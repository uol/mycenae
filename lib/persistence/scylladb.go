package persistence

import (
	"fmt"
	"time"

	"github.com/uol/logh"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/stats"
)

const structName string = "scylladb"

type scylladb struct {
	session         *gocql.Session
	logger          *logh.ContextualLogger
	timelineManager *stats.TimelineManager
	ksMngr          string
	grantUsername   string
	devMode         bool
	defaultTTL      int
}

func newScyllaPersistence(
	ksAdmin string,
	grantUsername string,
	session *gocql.Session,
	timelineManager *stats.TimelineManager,
	devMode bool,
	defaultTTL int,
) (Backend, error) {
	return &scylladb{
		session:         session,
		logger:          logh.CreateContextualLogger(constants.StringsPKG, "persistence"),
		timelineManager: timelineManager,
		ksMngr:          ksAdmin,
		grantUsername:   grantUsername,
		devMode:         devMode,
		defaultTTL:      defaultTTL,
	}, nil
}

const funcCreateKeyspace string = "CreateKeyspace"

func (backend *scylladb) CreateKeyspace(
	name, datacenter, contact string,
	replication int, ttl int,
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
		return errConflict(funcCreateKeyspace, structName,
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

	backend.statsQuery(keyspace.Name, constants.StringsEmpty, "create", time.Since(start))
	return nil
}

func (backend *scylladb) DeleteKeyspace(id string) gobol.Error {
	start := time.Now()
	query := fmt.Sprintf(formatDeleteKeyspace, id)
	if err := backend.session.Query(query).Exec(); err != nil {
		backend.statsQueryError(id, constants.StringsEmpty, "drop")
		return errPersist("DeleteKeyspace", structName, err)
	}

	backend.statsQuery(id, constants.StringsEmpty, "drop", time.Since(start))
	return nil
}

const (
	funcListKeyspaces  string = "ListKeyspaces"
	queryListKeyspaces string = `SELECT key, contact, datacenter, replication_factor FROM %s.ts_keyspace`
)

func (backend *scylladb) ListKeyspaces() ([]Keyspace, gobol.Error) {
	start := time.Now()
	iter := backend.session.Query(fmt.Sprintf(queryListKeyspaces, backend.ksMngr)).Iter()

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
				funcListKeyspaces,
				backend.ksMngr,
				scyllaSelect,
				time.Since(start),
			)
			return []Keyspace{}, errNoContent(
				funcListKeyspaces,
				structName,
			)
		}

		backend.statsQueryError(backend.ksMngr, "ts_keyspace", "select")
		return []Keyspace{}, errPersist(
			funcListKeyspaces,
			structName,
			err,
		)
	}

	backend.statsQuery(
		funcListKeyspaces,
		backend.ksMngr,
		scyllaSelect,
		time.Since(start),
	)

	return keyspaces, nil
}

const funcGetKeyspace string = "GetKeyspace"

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
		return Keyspace{}, false, errPersist(funcGetKeyspace, structName, err)
	}
	return ks, true, nil
}

const funcUpdateKeyspace string = "UpdateKeyspace"

func (backend *scylladb) UpdateKeyspace(ksid, contact string) gobol.Error {

	start := time.Now()

	query := fmt.Sprintf(formatUpdateKeyspace, backend.ksMngr)

	if _, found, err := backend.GetKeyspace(ksid); err != nil {
		return err
	} else if !found {
		return errNotFound(funcUpdateKeyspace, structName, constants.StringsEmpty)
	}

	if err := backend.session.Query(
		query, contact, ksid,
	).Exec(); err != nil {
		backend.statsQueryError(funcUpdateKeyspace, ksid, scyllaUpdate)
		return errPersist(funcUpdateKeyspace, structName, err)
	}

	backend.statsQuery(funcUpdateKeyspace, ksid, scyllaUpdate, time.Since(start))

	return nil
}

const funcListDatacenters string = "ListDatacenters"

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
		return nil, errPersist(funcListDatacenters, structName, err)
	}

	if len(datacenters) <= 0 {
		return datacenters, errNoContent(funcListDatacenters, structName)
	}

	return datacenters, nil
}
