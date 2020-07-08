package funks

import (
	"encoding/json"
	"errors"
	"time"
)

// Duration - a duration wrapper type to add the method below
type Duration struct {
	time.Duration
}

// NewStringDuration - returns a new duration based on a string
func NewStringDuration(value string) (*Duration, error) {

	d, err := time.ParseDuration(value)
	if err != nil {
		return nil, err
	}

	return &Duration{d}, nil
}

// ForceNewStringDuration - returns a new duration based on a string (panics on error)
func ForceNewStringDuration(value string) *Duration {

	d, err := time.ParseDuration(value)
	if err != nil {
		panic(err)
	}

	return &Duration{d}
}

// NewDuration - returns a new duration based on duration values
func NewDuration(value time.Duration) *Duration {

	return &Duration{value}
}

// UnmarshalText - used by the toml parser to proper parse duration values
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalJSON - transforms the value to the encoded json format
func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON - transforms the json format to the duration format
func (d *Duration) UnmarshalJSON(b []byte) error {

	var v interface{}
	var err error

	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {

	case float64:

		d.Duration = time.Duration(value)

		return nil

	case string:

		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}

		return nil

	default:
		return errors.New("invalid duration")
	}
}
