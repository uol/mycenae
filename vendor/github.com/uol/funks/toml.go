package funks

import "time"

// Duration - a duration wrapper type to add the method below
type Duration struct {
	time.Duration
}

// UnmarshalText - used by the toml parser to proper parse duration values
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
