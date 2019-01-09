package election

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// buildLogFields - build the log fields
func (e *Manager) buildLogFields(function string) []zapcore.Field {

	return []zapcore.Field{
		zap.String("package", "election"),
		zap.String("func", function),
	}
}

// logError - logs the error message
func (e *Manager) logError(function, message string) {

	e.logger.Error(message, e.buildLogFields(function)...)
}

// logInfo - logs the info message
func (e *Manager) logInfo(function, message string) {

	e.logger.Info(message, e.buildLogFields(function)...)
}
