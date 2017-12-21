package persistence

import "fmt"

const (
	scyllaMainKeyspace   = "mycenae"
	scyllaTestDatacenter = "datacenter1"
)

var (
	basicEnvironmentQueries = []string{
		fmt.Sprintf(`CREATE KEYSPACE %s WITH replication={
			'class': 'SimpleStrategy',
			'replication_factor': 1
		}`, scyllaMainKeyspace),
		fmt.Sprintf(`CREATE TABLE %s.ts_keyspace (
            key text PRIMARY KEY,
            name text,
            contact text,
            datacenter text,
            ks_ttl int
		)`, scyllaMainKeyspace),
		fmt.Sprintf(`CREATE TABLE %s.ts_datacenter (
			datacenter text PRIMARY KEY
		)`, scyllaMainKeyspace),
		fmt.Sprintf(`INSERT INTO %s.ts_datacenter (
			datacenter
		) VALUES ('%s')`, scyllaMainKeyspace, scyllaTestDatacenter),
	}
)
