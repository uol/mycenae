package keyspace

import (
	"regexp"

	"github.com/uol/gobol"
)

var emailRegex = regexp.MustCompile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$")

// Config is the json format for the keyspace configuration
type Config struct {
	Key               string `json:"key"`
	Name              string `json:"name"`
	Datacenter        string `json:"datacenter"`
	ReplicationFactor int    `json:"replicationFactor"`
	Contact           string `json:"contact"`
	TTL               int    `json:"ttl"`
}

// Validate checks if config is valid
func (c *Config) Validate() gobol.Error {

	if c.Datacenter == "" {
		return errValidationS("CreateKeyspace", "Datacenter cannot be empty or nil")
	}

	if c.ReplicationFactor <= 0 || c.ReplicationFactor > 3 {
		return errValidationS(
			"CreateKeyspace",
			"Replication factor cannot be less than or equal to 0 or greater than 3",
		)
	}

	if !emailRegex.MatchString(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	if c.TTL <= 0 {
		return errValidationS("CreateKeyspace", "TTL cannot be less or equal to zero")
	}

	return nil
}

// Validate checks if the update to the keyspace will be valid
func (c *ConfigUpdate) Validate() gobol.Error {

	if !emailRegex.MatchString(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	return nil
}

// ConfigUpdate is the json format for a keyspace update request
type ConfigUpdate struct {
	Contact string `json:"contact,omitempty"`
}

// CreateResponse is the json format for a keyspace creation endpoint response
type CreateResponse struct {
	Ksid string `json:"ksid,omitempty"`
}

// Response is a generic endpoint response
type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}
