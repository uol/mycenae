package validation

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/uol/gobol"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/utils"
)

// PropertyType - type
type PropertyType uint8

const (
	// TagKeyType - the tag identifier
	TagKeyType PropertyType = 1
	// TagValueType - the tag identifier
	TagValueType PropertyType = 2
	// MetricType - the metric identifier
	MetricType PropertyType = 3
)

// Service - the validation structure
type Service struct {
	configuration   *structs.ValidationConfiguration
	propertyRegexp  *regexp.Regexp
	keyspaceTTLMap  map[int]string
	metadataStorage *metadata.Storage
	logger          *logh.ContextualLogger
	defaultTTLStr   string
	defaultTTLTag   structs.TSDBTag
	keysetRegexp    *regexp.Regexp
}

// New - creates a new validation instance
func New(configuration *structs.ValidationConfiguration, metadataStorage *metadata.Storage, keyspaceTTLMap map[int]string) (*Service, error) {

	if configuration == nil {
		return nil, fmt.Errorf("validation configuration is null")
	}

	defaultTTLStr := strconv.Itoa(configuration.DefaultTTL)

	return &Service{
		configuration:   configuration,
		propertyRegexp:  regexp.MustCompile(configuration.PropertyRegexp),
		keysetRegexp:    regexp.MustCompile(configuration.KeysetNameRegexp),
		keyspaceTTLMap:  keyspaceTTLMap,
		metadataStorage: metadataStorage,
		logger:          logh.CreateContextualLogger(constants.StringsPKG, "validation"),
		defaultTTLStr:   defaultTTLStr,
		defaultTTLTag:   structs.TSDBTag{Name: constants.StringsTTL, Value: defaultTTLStr},
	}, nil
}

// ValidateType - validates the point type
func (v *Service) ValidateType(p *structs.TSDBpoint, number bool) gobol.Error {

	if number {
		if p.Value == nil {
			return errNumberTypeExpected
		}
	} else {
		if p.Text == constants.StringsEmpty {
			return errTextTypeExpected
		}

		if len(p.Text) > v.configuration.MaxTextValueSize {
			return errMaxTextValueSize
		}
	}

	return nil
}

// ValidateProperty - validates the property value
func (v *Service) ValidateProperty(value string, propertyType PropertyType) gobol.Error {

	isValid := len(value) < v.configuration.MaxPropertySize
	isValid = isValid && v.propertyRegexp.MatchString(value)

	if !isValid {
		switch propertyType {
		case TagKeyType:
			return errInvalidTagKey
		case TagValueType:
			return errInvalidTagValue
		case MetricType:
			return errInvalidMetric
		default:
			if logh.ErrorEnabled {
				v.logger.Error().Msgf("no property type of value %d is mapped", propertyType)
			}
			return errInvalidPropertyType
		}
	}

	return nil
}

// ValidateTags - validates the tags from the point
func (v *Service) ValidateTags(p *structs.TSDBpoint) gobol.Error {

	lt := len(p.Tags)

	if lt == 0 {
		return errNoTags
	}

	if lt == 2 && p.TTL != 0 && p.Keyset != constants.StringsEmpty {
		return errNoUserTags
	}

	return nil
}

// ValidateKeyset - validates the keyset
func (v *Service) ValidateKeyset(keyset string) gobol.Error {

	if keyset == constants.StringsEmpty {
		return errNoKeysetTag
	}

	if !v.keysetRegexp.MatchString(keyset) {
		return errInvalidKeysetFormat
	}

	keysetExists, gerr := v.metadataStorage.CheckKeyset(keyset)
	if gerr != nil {
		return gerr
	}

	if !keysetExists {
		return errInexistentKeyset
	}

	return nil
}

// ParseTTL - parses the TTL and returns its int value
func (v *Service) ParseTTL(value string) (int, string, gobol.Error) {
	if value == constants.StringsEmpty {
		return v.configuration.DefaultTTL, v.defaultTTLStr, nil
	}

	ttl, err := strconv.Atoi(value)
	if err != nil {
		return 0, constants.StringsEmpty, errInvalidTTLValue
	}

	if _, ok := v.keyspaceTTLMap[ttl]; !ok {
		return v.configuration.DefaultTTL, v.defaultTTLStr, nil
	}

	return ttl, value, nil
}

const (
	cFuncParseTimestamp string = "ParseTimestamp"
	cMsgParseTimestamp  string = "Error parsing timestamp."
)

// ValidateTimestamp - parses the timestamp
func (v *Service) ValidateTimestamp(timestamp int64) (int64, gobol.Error) {
	if timestamp == 0 {
		return utils.GetTimeNoMillis(), nil
	}

	truncated, err := utils.MilliToSeconds(timestamp)
	if err != nil {
		return 0, errBadRequest(cFuncParseTimestamp, cMsgParseTimestamp, err)
	}

	return truncated, nil
}

// GetDefaultTTLTag - returns the default TTL tag and its integer value
func (v *Service) GetDefaultTTLTag() (*structs.TSDBTag, int) {

	return &v.defaultTTLTag, v.configuration.DefaultTTL
}
