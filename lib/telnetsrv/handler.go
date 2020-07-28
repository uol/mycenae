package telnetsrv

import (
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/validation"
)

//
// Specifies a telnet data handler
// author: rnojiri
//

// TelnetDataHandler - handles the data from the telnet interface
type TelnetDataHandler interface {

	// Handle - handles the data and send
	Handle(line, ip string) bool

	// GetSourceType - returns the source type
	GetSourceType() *constants.SourceType

	// GetLogger - returns the logger
	GetLogger() *logh.ContextualLogger

	// GetValidationService - returns the validation service instance
	GetValidationService() *validation.Service

	// GetConfiguration - returns this handler configuration
	GetConfiguration() *structs.TelnetServerConfiguration
}
