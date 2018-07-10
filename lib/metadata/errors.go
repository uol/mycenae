package metadata

import (
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			code,
			[]zapcore.Field{
				zap.String("package", "metadata"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errConflict(f string, err error) gobol.Error {
	return errBasic(f, "", http.StatusConflict, err)
}

func errInternalServer(f string, err error) gobol.Error {
	return errBasic(f, "", http.StatusInternalServerError, err)
}
