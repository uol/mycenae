package keyspace

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
)

type persistence struct {
	cassandra     *gocql.Session
	usernameGrant string
	keyspaceMain  string
}

func (persist *persistence) createTable(keyspace string, ttl uint8, number bool) error {

	var valueColumnType, tableName string

	if number {
		valueColumnType = "double"
		tableName = "ts_number_stamp"
	} else {
		valueColumnType = "text"
		tableName = "ts_text_stamp"
	}

	tableTTL := uint64(ttl) * 86400

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s.%s (id text, date timestamp, value %s, PRIMARY KEY (id, date))
			 WITH CLUSTERING ORDER BY (date ASC)
			 AND bloom_filter_fp_chance = 0.01
			 AND caching = {'keys':'ALL', 'rows_per_partition':'ALL'}
			 AND comment = ''
			 AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'SECONDS', 'base_time_seconds':'3600', 'max_sstable_age_days':'180'}
			 AND compression = {'crc_check_chance': '0.25', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor', 'chunk_length_kb': 1}
			 AND dclocal_read_repair_chance = 0.0
			 AND default_time_to_live = %d
			 AND gc_grace_seconds = 0
			 AND max_index_interval = 2048
			 AND memtable_flush_period_in_ms = 0
			 AND min_index_interval = 128
			 AND read_repair_chance = 0.0
			 AND speculative_retry = '99.0PERCENTILE'`,
		keyspace,
		tableName,
		valueColumnType,
		tableTTL,
	)

	if err := persist.cassandra.Query(
		query,
	).Exec(); err != nil {
		return err
	}

	return nil
}

func (persist *persistence) createKeyspace(ksc Config) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`CREATE KEYSPACE %s
			 WITH replication={'class':'NetworkTopologyStrategy', '%s':%d} AND durable_writes=true`,
			ksc.Name,
			ksc.Datacenter,
			ksc.ReplicationFactor,
		),
	).Exec(); err != nil {
		statsQueryError(ksc.Name, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.createTable(ksc.Name, ksc.TTL, true); err != nil {
		statsQueryError(ksc.Name, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.createTable(ksc.Name, ksc.TTL, false); err != nil {
		statsQueryError(ksc.Name, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.cassandra.Query(
		fmt.Sprintf(`GRANT MODIFY ON KEYSPACE %s TO %s`, ksc.Name, persist.usernameGrant),
	).Exec(); err != nil {
		statsQueryError(ksc.Name, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.cassandra.Query(
		fmt.Sprintf(`GRANT SELECT ON KEYSPACE %s TO %s`, ksc.Name, persist.usernameGrant),
	).Exec(); err != nil {
		statsQueryError(ksc.Name, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	statsQuery(ksc.Name, "", "create", time.Since(start))
	return nil
}

func (persist *persistence) createKeyspaceMeta(ksc Config) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`INSERT INTO %s.ts_keyspace (key, contact, datacenter, replication_factor) VALUES (?, ?, ?, ?)`,
			persist.keyspaceMain,
		),
		ksc.Name,
		ksc.Contact,
		ksc.Datacenter,
		ksc.ReplicationFactor,
	).Exec(); err != nil {
		statsQueryError(persist.keyspaceMain, "ts_keyspace", "insert")
		return errPersist("CreateKeyspaceMeta", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "insert", time.Since(start))
	return nil
}

func (persist *persistence) updateKeyspace(ksc ConfigUpdate, key string) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(`UPDATE %s.ts_keyspace SET contact = ? WHERE key = ?`, persist.keyspaceMain),
		ksc.Contact,
		key,
	).Exec(); err != nil {
		statsQueryError(persist.keyspaceMain, "ts_keyspace", "update")
		return errPersist("UpdateKeyspace", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "update", time.Since(start))
	return nil
}

func (persist *persistence) countByValueInColumn(column string, table string, namespace string, funcName string, value string) (int, gobol.Error) {

	start := time.Now()

	it := persist.cassandra.Query(fmt.Sprintf("SELECT %s FROM %s.%s", column, namespace, table)).Iter()

	var count int
	var scanned string
	for it.Scan(&scanned) {
		if value == scanned {
			count++
		}
	}

	if err := it.Close(); err != nil {
		statsQueryError(namespace, table, "select")
		return 0, errPersist(funcName, err)
	}

	statsQuery(namespace, table, "select", time.Since(start))

	return count, nil
}

func (persist *persistence) countKeyspaceByKey(key string) (int, gobol.Error) {

	return persist.countByValueInColumn("key", "ts_keyspace", persist.keyspaceMain, "countKeyspaceByKey", key)
}

func (persist *persistence) countDatacenterByName(name string) (int, gobol.Error) {

	return persist.countByValueInColumn("datacenter", "ts_datacenter", persist.keyspaceMain, "countDatacenterByName", name)
}

func (persist *persistence) dropKeyspace(key string) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(`DROP KEYSPACE IF EXISTS %s`, key),
	).Exec(); err != nil {
		statsQueryError(key, "", "drop")
		return errPersist("DropKeyspace", err)
	}

	statsQuery(key, "", "drop", time.Since(start))
	return nil
}

func (persist *persistence) getKeyspace(key string) (Config, bool, gobol.Error) {
	start := time.Now()

	var datacenter string
	var replication int

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT datacenter, replication_factor FROM %s.ts_keyspace WHERE key = ?`,
			persist.keyspaceMain,
		),
		key,
	).Scan(&datacenter, &replication); err != nil {

		if err == gocql.ErrNotFound {
			statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
			return Config{}, false, errNotFound("GetKeyspace")
		}

		statsQueryError(persist.keyspaceMain, "ts_keyspace", "select")
		return Config{}, false, errPersist("GetKeyspace", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
	return Config{
		Name:              key,
		Datacenter:        datacenter,
		ReplicationFactor: replication,
	}, true, nil
}

func (persist *persistence) checkKeyspace(key string) gobol.Error {
	start := time.Now()

	count, err := persist.countByValueInColumn("key", "ts_keyspace", persist.keyspaceMain, "checkKeyspace", key)

	if err != nil {
		return err
	}

	if count > 0 {
		statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
		return nil
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
	return errNotFound("CheckKeyspace")
}

func (persist *persistence) listAllKeyspaces() ([]Config, gobol.Error) {
	start := time.Now()

	iter := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT key, contact, datacenter, replication_factor FROM %s.ts_keyspace`,
			persist.keyspaceMain,
		),
	).Iter()

	var key, name, contact, datacenter string
	var replication int

	keyspaces := []Config{}

	for iter.Scan(&key, &contact, &datacenter, &replication) {

		keyspaceMsg := Config{
			Name:              name,
			Contact:           contact,
			Datacenter:        datacenter,
			ReplicationFactor: replication,
		}
		if keyspaceMsg.Name != persist.keyspaceMain {
			keyspaces = append(keyspaces, keyspaceMsg)
		}
	}

	if err := iter.Close(); err != nil {

		if err == gocql.ErrNotFound {
			statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
			return []Config{}, errNoContent("ListAllKeyspaces")
		}

		statsQueryError(persist.keyspaceMain, "ts_keyspace", "select")
		return []Config{}, errPersist("ListAllKeyspaces", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
	return keyspaces, nil
}

func (persist *persistence) listDatacenters() ([]string, gobol.Error) {
	start := time.Now()

	iter := persist.cassandra.Query("SELECT * FROM ts_datacenter").Iter()

	var name string
	dcs := []string{}

	for iter.Scan(&name) {
		dcs = append(dcs, name)
	}

	if err := iter.Close(); err != nil {

		if err == gocql.ErrNotFound {
			statsQuery(persist.keyspaceMain, "ts_datacenter", "select", time.Since(start))
			return []string{}, errNoContent("ListDatacenters")
		}

		statsQueryError(persist.keyspaceMain, "ts_datacenter", "select")
		return []string{}, errPersist("ListDatacenters", err)
	}

	return dcs, nil
}
