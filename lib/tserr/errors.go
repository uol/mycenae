package tserr

import (
	"github.com/uol/gobol"
	"go.uber.org/zap/zapcore"
)

func New(e error, msg string, httpCode int, lf []zapcore.Field) gobol.Error {
	return customError{
		e,
		msg,
		httpCode,
		lf,
	}
}

type customError struct {
	error
	msg      string
	httpCode int
	lf       []zapcore.Field
}

func (e customError) Message() string {
	return e.msg
}

func (e customError) StatusCode() int {
	return e.httpCode
}

func (e customError) LogFields() []zapcore.Field {
	return e.lf
}
