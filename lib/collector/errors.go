package collector

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errBR(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusBadRequest,
			[]zapcore.Field{
				zap.String("package", "collector"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errISE(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusInternalServerError,
			[]zapcore.Field{
				zap.String("package", "collector"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errValidationTelnet(s string) gobol.Error {
	return errBR("validateTelnetFormat", s, errors.New(s))
}

func errValidation(s string) gobol.Error {
	return errBR("makePacket", s, errors.New(s))
}

func errUnmarshal(f string, e error) gobol.Error {
	return errBR(f, "Wrong JSON format", e)
}

func errMarshal(f string, e error) gobol.Error {
	return errISE(f, e.Error(), e)
}

func errPersist(f string, e error) gobol.Error {
	return errISE(f, e.Error(), e)
}
