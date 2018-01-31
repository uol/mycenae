package persistence

// Keyspace represents a keyspace within the database
type Keyspace struct {
	// Name is a human-friendly name for the keyspace
	Name string `json:"name"`
	// Contact should be an email address for an owner of the keyspace
	Contact string `json:"contact"`
	// DC is the datacenter where the keyspace should reside
	DC string `json:"datacenter"`
	// TTL is the time-to-live for the keyspace data
	TTL uint8 `json:"ttl"`
	// --- This will be removed ---
	Replication int `json:"replicationFactor"`
}
