package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUDPv1PayloadWithAllFieldsAndOneTagSTAMP(t *testing.T) {

	payload := `{
    	"customer": "mycenae",
    	"entity": "mycenae1",
    	"metric": "sys.cpu",
    	"value": 100.8,
    	"label": "service started",
    	"ttl": 30
	}`

	err := mycenaeTools.UDP.SendString(payload)
	assert.NoError(t, err)
}
