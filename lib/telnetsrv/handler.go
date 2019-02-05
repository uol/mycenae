package telnetsrv

import (
	"github.com/uol/mycenae/lib/collector"
	"go.uber.org/zap"
)

// TelnetDataHandler - handles the data from the telnet interface
type TelnetDataHandler interface {

	// Handle - handles the data and send
	Handle(data *string, collector *collector.Collector, logger *zap.Logger)
}
