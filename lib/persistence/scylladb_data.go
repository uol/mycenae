package persistence

const formatAddKeyspace = `INSERT INTO %s.ts_keyspace (key, contact, datacenter, replication_factor, creation_date) VALUES (?, ?, ?, ?, dateof(now()))`

const formatCreateKeyspace = `
    CREATE KEYSPACE %s WITH replication={
        'class': 'NetworkTopologyStrategy',
        '%s': %d
    } AND durable_writes=true
`

const formatCreateTable = `
	CREATE TABLE IF NOT EXISTS %s.%s (id text, date timestamp, value %s, PRIMARY KEY (id, date))
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
	AND speculative_retry = '99.0PERCENTILE'
`
const formatDeleteKeyspace = `DROP KEYSPACE IF EXISTS %s`

const formatGetKeyspace = `SELECT key, contact, datacenter, replication_factor FROM %s.ts_keyspace WHERE key = ?`

var formatGrants = []string{
	`GRANT MODIFY ON KEYSPACE %s TO %s`,
	`GRANT SELECT ON KEYSPACE %s TO %s`,
}

const formatUpdateKeyspace = `UPDATE %s.ts_keyspace SET contact = ? WHERE key = ?`

const formatListDatacenters = `SELECT datacenter FROM %s.ts_datacenter`
