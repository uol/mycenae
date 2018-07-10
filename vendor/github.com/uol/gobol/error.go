package gobol

import (
	"go.uber.org/zap/zapcore"
)

type Error interface {
	error
	StatusCode() int
	Message() string
	LogFields() []zapcore.Field
}
