package structs

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusBadRequest,
			[]zapcore.Field{
				zap.String("package", "structs"),
				zap.String("func", f),
			},
		)
	}
	return nil
}

func errValidation(e error) gobol.Error {
	return errBasic("CheckTSDBquery", e.Error(), e)
}

func errCheckDuration(e error) gobol.Error {
	return errBasic("CheckDuration", e.Error(), e)
}

func errField(s string) gobol.Error {
	return errBasic("CheckField", s, errors.New(s))

}

func errFilterField(s string) gobol.Error {
	return errBasic("CheckField", s, errors.New(s))
}

func errAggregator(s string) gobol.Error {
	return errBasic("CheckAggregator", s, errors.New(s))
}

func errDownsampler(s string) gobol.Error {
	return errBasic("CheckDownsampler", s, errors.New(s))
}

func errFiller(s string) gobol.Error {
	return errBasic("CheckFiller", s, errors.New(s))
}

func errRate(s string) gobol.Error {
	return errBasic("CheckRate", s, errors.New(s))
}

func errFilter(s string) gobol.Error {
	return errBasic("CheckFilter", s, errors.New(s))
}

func errValidationS(f, s string) gobol.Error {
	return errBasic(f, s, errors.New(s))
}
