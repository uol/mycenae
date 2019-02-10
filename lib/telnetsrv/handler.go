package telnetsrv

import (
	"github.com/uol/mycenae/lib/collector"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TelnetDataHandler - handles the data from the telnet interface
type TelnetDataHandler interface {

	// Handle - handles the data and send
	Handle(line string, collector *collector.Collector, logger *zap.Logger, loggerFields []zapcore.Field)
}
