CREATE KEYSPACE mycenae WITH replication = {'class':'NetworkTopologyStrategy', 'dc_gt_a1': 2} AND durable_writes = true;

CREATE TABLE IF NOT EXISTS mycenae.ts_keyspace (key text PRIMARY KEY, contact text, datacenter text, replication_factor int);

CREATE TABLE IF NOT EXISTS mycenae.ts_datacenter (datacenter text PRIMARY KEY);

INSERT INTO mycenae.ts_keyspace (key, datacenter, contact, replication_factor) VALUES ('mycenae', 'dc_gt_a1', 'l-pd-plat-servicos@uolinc.com', 2);

INSERT INTO mycenae.ts_datacenter (datacenter) VALUES ('dc_gt_a1');