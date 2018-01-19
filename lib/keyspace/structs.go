package keyspace

import (
	"github.com/asaskevich/govalidator"
	"github.com/uol/gobol"
)

type Config struct {
	Name              string `json:"name"`
	Datacenter        string `json:"datacenter"`
	ReplicationFactor int    `json:"replicationFactor"`
	Contact           string `json:"contact"`
	TTL               uint8  `json:"ttl"`
}

func (c *Config) Validate() gobol.Error {

	if c.Datacenter == "" {
		return errValidationS("CreateKeyspace", "Datacenter can not be empty or nil")
	}

	if c.ReplicationFactor <= 0 || c.ReplicationFactor > 3 {
		return errValidationS(
			"CreateKeyspace",
			"Replication factor can not be less than or equal to 0 or greater than 3",
		)
	}

	if !govalidator.IsEmail(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	if c.TTL <= 0 {
		return errValidationS("CreateKeyspace", "TTL cannot be less or equal to 0")
	}

	return nil
}

func (c *ConfigUpdate) Validate() gobol.Error {

	if !govalidator.IsEmail(c.Contact) {
		return errValidationS("CreateKeyspace", "Contact field should be a valid email address")
	}

	return nil
}

type ConfigUpdate struct {
	Contact string `json:"contact,omitempty"`
}

type CreateResponse struct {
	Ksid string `json:"ksid,omitempty"`
}

type Response struct {
	TotalRecords int         `json:"totalRecords,omitempty"`
	Payload      interface{} `json:"payload,omitempty"`
	Message      interface{} `json:"message,omitempty"`
}
