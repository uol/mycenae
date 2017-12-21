CREATE KEYSPACE mycenae WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}  AND durable_writes = true;

CREATE KEYSPACE stats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_gt_a1': '3'}  AND durable_writes = true;

CREATE TABLE IF NOT EXISTS mycenae.ts_keyspace (key text PRIMARY KEY, contact text, datacenter text, ks_ttl int, ks_tuuid boolean, name text, replication_factor int, replication_factor_meta text);

CREATE TABLE IF NOT EXISTS mycenae.ts_datacenter (datacenter text PRIMARY KEY);

CREATE TABLE stats.ts_text_stamp (
    id text,
    date timestamp,
    value text,
    PRIMARY KEY (id, date)
) WITH CLUSTERING ORDER BY (date ASC)
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'SECONDS', 'base_time_seconds':'3600', 'max_sstable_age_days':'365'}
    AND dclocal_read_repair_chance = 1.0
    AND default_time_to_live = 43200
    AND gc_grace_seconds = 0
    AND read_repair_chance = 1.0
    AND speculative_retry = 'NONE';

CREATE TABLE IF NOT EXISTS stats.ts_number_stamp (
    id text,
    date timestamp,
    value double,
    PRIMARY KEY (id, date)
)
WITH CLUSTERING ORDER BY (date ASC)
    AND compaction = {'class': 'DateTieredCompactionStrategy', 'timestamp_resolution':'SECONDS', 'base_time_seconds':'3600', 'max_sstable_age_days':'365'}
    AND dclocal_read_repair_chance = 1.0
    AND default_time_to_live = 43200
    AND gc_grace_seconds = 0
    AND read_repair_chance = 1.0
    AND speculative_retry = 'NONE';

INSERT INTO mycenae.ts_keyspace (key, datacenter, name, replication_factor) VALUES ('mycenae', 'dc_gt_a1', 'mycenae', 2);

INSERT INTO mycenae.ts_datacenter (datacenter) VALUES ('dc_gt_a1');

INSERT INTO mycenae.ts_keyspace (key, datacenter, ks_ttl, ks_tuuid, name, replication_factor) VALUES ('stats', 'dc_gt_a1', 3, false, 'stats', 2);