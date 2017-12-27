package persistence

const formatAddKeyspace = `INSERT INTO %s.ts_keyspace (key, name, contact, datacenter, ks_ttl, replication_factor, replication_factor_meta) VALUES (?, ?, ?, ?, ?, ?, ?)`

const formatCreateKeyspace = `
    CREATE KEYSPACE %s WITH replication={
        'class': 'NetworkTopologyStrategy',
        '%s': %d
    } AND durable_writes=true
`

const formatCreateNumericTable = `
    CREATE TABLE IF NOT EXISTS %s.ts_number_stamp (
        id text,
        date timestamp,
        value double,
        PRIMARY KEY (id, date)
    ) WITH CLUSTERING ORDER BY (date ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {
        'keys':'ALL',
        'rows_per_partition':'NONE'
    } AND comment = ''
    AND compaction={
        'timestamp_resolution': 'SECONDS',
        'base_time_seconds': '3600',
        'max_sstable_age_days': '365',
        'class': '%s'
    }
    AND compression = {
        'crc_check_chance': '0.5',
        'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'
    } AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = %d
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE'
`

const formatCreateTextTable = `
    CREATE TABLE IF NOT EXISTS %s.ts_text_stamp (
        id text, date timestamp,
        value text,
        PRIMARY KEY (id, date)
    ) WITH CLUSTERING ORDER BY (date ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {
        'keys':'ALL',
        'rows_per_partition':'NONE'
    } AND comment = ''
    AND compaction={
        'timestamp_resolution': 'SECONDS',
        'base_time_seconds': '3600',
        'max_sstable_age_days': '365',
        'class': '%s'
    } AND compression = {
        'crc_check_chance': '0.5',
        'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'
    } AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = %d
    AND gc_grace_seconds = 0
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE'
`

const formatDeleteKeyspace = `DROP KEYSPACE IF EXISTS %s`

const formatGetKeyspace = `SELECT name, contact, datacenter, ks_ttl, replication_factor FROM %s.ts_keyspace WHERE key = ?`

var formatGrants = []string{
	`GRANT MODIFY ON KEYSPACE %s TO %s`,
	`GRANT SELECT ON KEYSPACE %s TO %s`,
}

const formatUpdateKeyspace = `UPDATE %s.ts_keyspace SET name = ?, contact = ? WHERE key = ?`

const formatListDatacenters = `SELECT datacenter FROM %s.ts_datacenter`
