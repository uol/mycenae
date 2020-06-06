package validation

import (
	"strings"

	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	tlmanager "github.com/uol/timelinemanager"
)

//
// Statistics for the validation errors.
// @author: rnojiri
//

type idType string

const (
	metricValidationError string                = "point.validation"
	metricValidationCount string                = "point.validation.count"
	tagErrorCode          string                = "code"
	tagIDType             string                = "id_type"
	idTypeKeyset          idType                = "keyset"
	idTypeIP              idType                = "ip"
	idTypeBoth            idType                = "keyset_ip"
	validationStorage     tlmanager.StorageType = "validation"
)

var (
	noTags = []interface{}{}
)

// StatsValidationError - accumulates validation data
func (v *Service) StatsValidationError(function, keyset, ip string, sourceType *constants.SourceType, gerr gobol.Error) {

	errorCode := gerr.ErrorCode()

	// ignore if no error code
	if len(errorCode) == 0 {
		return
	}

	noKeyset := len(keyset) == 0
	if noKeyset {
		keyset = constants.StringsUnknown
	}

	noIP := len(ip) == 0
	if noIP {
		ip = constants.StringsUnknown
	}

	if noKeyset && noIP {

		if logh.WarnEnabled {
			ev := v.logger.Warn()
			if len(function) > 0 {
				ev.Str(constants.StringsFunc, function)
			}
			ev.Msgf("cannot send validation error statistics code \"%s\": no ip or keyset found", errorCode)
		}

		return
	}

	tags := []interface{}{}
	var metricIDType idType
	var fullErrorCode string

	b := strings.Builder{}
	b.Grow(len(sourceType.ErrorCodePrefix) + len(errorCode))
	b.WriteString(sourceType.ErrorCodePrefix)
	b.WriteString(errorCode)
	fullErrorCode = b.String()

	b.Reset()
	b.Grow(len(keyset) + len(ip) + len(fullErrorCode))

	if !noKeyset {
		b.WriteString(keyset)
		metricIDType = idTypeKeyset
	}

	if !noIP {
		b.WriteString(ip)
		tags = append(tags, constants.StringsIP, ip)
		metricIDType = idTypeIP
	}

	b.WriteString(fullErrorCode)
	key := b.String()

	if !noKeyset && !noIP {
		metricIDType = idTypeBoth
	}

	tags = append(tags, tagIDType, metricIDType, tagErrorCode, fullErrorCode, constants.StringsTargetKSID, keyset)

	stored, err := v.timelineManager.AccumulateHashedData(validationStorage, key)
	if err != nil {

		if logh.ErrorEnabled {
			ev := v.logger.Error()

			if len(function) > 0 {
				ev.Str(constants.StringsFunc, function)
			}

			ev.Str(constants.StringsFunc, function).Err(err).Msgf("error incrementing validation error count: %s, %s, %s", keyset, ip, errorCode)
		}

		return
	}

	if stored {
		if logh.DebugEnabled {
			v.logger.Debug().Str(constants.StringsFunc, function).Msgf("validation error incremented: %s, %s, %s", keyset, ip, errorCode)
		}

		v.incValidationErrorCount()
		return
	}

	err = v.timelineManager.StoreDefaultTTLCustomHash(
		validationStorage,
		key,
		metricValidationError,
		tags...,
	)

	if err != nil {

		if logh.ErrorEnabled {
			ev := v.logger.Error()

			if len(function) > 0 {
				ev.Str(constants.StringsFunc, function)
			}

			ev.Str(constants.StringsFunc, function).Err(err).Msgf("error storing accumulated metric: %s, %s, %s", keyset, ip, errorCode)
		}

		return
	}

	if logh.DebugEnabled {
		v.logger.Debug().Str(constants.StringsFunc, function).Msgf("accumulated metric stored with success: %s, %s, %s", keyset, ip, errorCode)
	}
}

// storeValidationErrorCount - stores the global validation error counter
func (v *Service) storeValidationErrorCount() {

	v.timelineManager.StoreHashedData(
		validationStorage,
		metricValidationCount,
		0,
		metricValidationCount,
		noTags...,
	)

	if logh.InfoEnabled {
		v.logger.Info().Msg("validation error counter stored")
	}
}

// incValidationErrorCount - increments the validation error counter
func (v *Service) incValidationErrorCount() {

	v.timelineManager.AccumulateHashedData(
		validationStorage,
		metricValidationCount,
	)
}
