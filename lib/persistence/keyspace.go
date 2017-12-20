package persistence

// Keyspace represents a keyspace within the database
type Keyspace struct {
	// ID is a unique identifier for the keyspace. It should be the thing
	// that is used to send points to the keyspace
	ID string `json:"key"`
	// Name is a human-friendly name for the keyspace
	Name string `json:"name"`
	// Contact should be an email address for an owner of the keyspace
	Contact string `json:"contact"`
	// DC is the datacenter where the keyspace should reside
	DC string `json:"datacenter"`
	// TTL is the time-to-live for the keyspace data
	TTL int `json:"ttl"`
}
