package keyspace

import (
	"bytes"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

type persistence struct {
	cassandra     *gocql.Session
	esearch       *rubber.Elastic
	usernameGrant string
	keyspaceMain  string
	compaction    string
}

func (persist *persistence) createKeyspace(ksc Config, key string) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`CREATE KEYSPACE %s
			 WITH replication={'class':'NetworkTopologyStrategy', '%s':%d} AND durable_writes=true`,
			key,
			ksc.Datacenter,
			ksc.ReplicationFactor,
		),
	).Exec(); err != nil {
		statsQueryError(key, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	defaultTTL := ksc.TTL * 86400

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.ts_number_stamp (id text, date timestamp, value double, PRIMARY KEY (id, date))
				 WITH CLUSTERING ORDER BY (date ASC)
				 AND bloom_filter_fp_chance = 0.01
				 AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
				 AND comment = ''
				 AND compaction={ 'min_threshold': '8', 'max_threshold': '64', 'compaction_window_unit': 'DAYS', 'compaction_window_size': '7', 'class': '%s'}
				 AND compression = {'crc_check_chance': '0.5', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
				 AND dclocal_read_repair_chance = 0.0
				 AND default_time_to_live = %d
				 AND gc_grace_seconds = 0
				 AND max_index_interval = 2048
				 AND memtable_flush_period_in_ms = 0
				 AND min_index_interval = 128
				 AND read_repair_chance = 0.0
				 AND speculative_retry = '99.0PERCENTILE'`,
			key,
			persist.compaction,
			defaultTTL,
		),
	).Exec(); err != nil {
		statsQueryError(key, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.ts_text_stamp (id text, date timestamp, value text, PRIMARY KEY (id, date))
				 WITH CLUSTERING ORDER BY (date ASC)
				 AND bloom_filter_fp_chance = 0.01
				 AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
				 AND comment = ''
				 AND compaction={ 'min_threshold': '8', 'max_threshold': '64', 'compaction_window_unit': 'DAYS', 'compaction_window_size': '7', 'class': '%s'}
				 AND compression = {'crc_check_chance': '0.5', 'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
				 AND dclocal_read_repair_chance = 0.0
				 AND default_time_to_live = %d
				 AND gc_grace_seconds = 0
				 AND max_index_interval = 2048
				 AND memtable_flush_period_in_ms = 0
				 AND min_index_interval = 128
				 AND read_repair_chance = 0.0
				 AND speculative_retry = '99.0PERCENTILE'`,
			key,
			persist.compaction,
			defaultTTL,
		),
	).Exec(); err != nil {
		statsQueryError(key, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.cassandra.Query(
		fmt.Sprintf(`GRANT MODIFY ON KEYSPACE %s TO %s`, key, persist.usernameGrant),
	).Exec(); err != nil {
		statsQueryError(key, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	if err := persist.cassandra.Query(
		fmt.Sprintf(`GRANT SELECT ON KEYSPACE %s TO %s`, key, persist.usernameGrant),
	).Exec(); err != nil {
		statsQueryError(key, "", "create")
		return errPersist("CreateKeyspace", err)
	}

	statsQuery(key, "", "create", time.Since(start))
	return nil
}

func (persist *persistence) createKeyspaceMeta(ksc Config, key string) gobol.Error {
	start := time.Now()

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`INSERT INTO %s.ts_keyspace (key, name, contact, replication_factor, datacenter, ks_ttl) VALUES (?, ?, ?, ?, ?, ?)`,
			persist.keyspaceMain,
		),
		key,
		ksc.Name,
		ksc.Contact,
		ksc.ReplicationFactor,
		ksc.Datacenter,
		ksc.TTL,
		//ksc.TUUID,
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
		fmt.Sprintf(`UPDATE %s.ts_keyspace SET name = ?, contact = ? WHERE key = ?`, persist.keyspaceMain),
		ksc.Name,
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

func (persist *persistence) countKeyspaceByName(name string) (int, gobol.Error) {

	return persist.countByValueInColumn("name", "ts_keyspace", persist.keyspaceMain, "countKeyspaceByName", name)
}

func (persist *persistence) getKeyspaceKeyByName(name string) (string, gobol.Error) {
	start := time.Now()

	var key string

	if err := persist.cassandra.Query(
		fmt.Sprintf(`SELECT key FROM %s.ts_keyspace WHERE name = ? `, persist.keyspaceMain),
		name,
	).Scan(&key); err != nil {

		if err == gocql.ErrNotFound {
			statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
			return "", errNotFound("GetKeyspaceKeyByName")
		}

		statsQueryError(persist.keyspaceMain, "ts_keyspace", "select")
		return key, errPersist("GetKeyspaceKeyByName", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
	return key, nil
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

	var name, datacenter string
	var replication, ttl int

	if err := persist.cassandra.Query(
		fmt.Sprintf(
			`SELECT name, datacenter, replication_factor, ks_ttl FROM %s.ts_keyspace WHERE key = ?`,
			persist.keyspaceMain,
		),
		key,
	).Scan(&name, &datacenter, &replication, &ttl); err != nil {

		if err == gocql.ErrNotFound {
			statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
			return Config{}, false, errNotFound("GetKeyspace")
		}

		statsQueryError(persist.keyspaceMain, "ts_keyspace", "select")
		return Config{}, false, errPersist("GetKeyspace", err)
	}

	statsQuery(persist.keyspaceMain, "ts_keyspace", "select", time.Since(start))
	return Config{
		Key:               key,
		Name:              name,
		Datacenter:        datacenter,
		ReplicationFactor: replication,
		TTL:               ttl,
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
			`SELECT key, name, contact, datacenter, replication_factor, ks_ttl FROM %s.ts_keyspace`,
			persist.keyspaceMain,
		),
	).Iter()

	var key, name, contact, datacenter string
	var replication, ttl int

	keyspaces := []Config{}

	for iter.Scan(&key, &name, &contact, &datacenter, &replication, &ttl) {

		keyspaceMsg := Config{
			Key:               key,
			Name:              name,
			Contact:           contact,
			Datacenter:        datacenter,
			ReplicationFactor: replication,
			TTL:               ttl,
		}
		if keyspaceMsg.Key != persist.keyspaceMain {
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

func (persist *persistence) createIndex(esIndex string) gobol.Error {
	start := time.Now()

	body := &bytes.Buffer{}

	body.WriteString(
		`{"mappings":{"meta":{"properties":{"tagsNested":{"type":"nested","properties":{"tagKey":{"type":"string"},"tagValue":{"type":"string"}}}}},"metatext":{"properties":{"tagsNested":{"type":"nested","properties":{"tagKey":{"type":"string"},"tagValue":{"type":"string"}}}}}}}`,
	)

	_, err := persist.esearch.CreateIndex(esIndex, body)
	if err != nil {
		statsIndexError(esIndex, "", "post")
		return errPersist("CreateIndex", err)
	}

	statsIndex(esIndex, "", "post", time.Since(start))
	return nil
}

func (persist *persistence) deleteIndex(esIndex string) gobol.Error {
	start := time.Now()

	_, err := persist.esearch.DeleteIndex(esIndex)
	if err != nil {
		statsIndexError(esIndex, "", "delete")
		return errPersist("DeleteIndex", err)
	}

	statsIndex(esIndex, "", "delete", time.Since(start))
	return nil
}
