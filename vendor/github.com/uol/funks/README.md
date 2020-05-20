# Funks
This library provides useful functions for any GO projects.

## Sync
##### func GetSyncMapSize(m *sync.Map) int
Returns the number of items from the specified sync.Map. 

## TOML
##### funks.Duration
Implements the function *"UnmarshalText(text []byte) error"* from the encoding.TextUnmarshaler interface to parse *"time.Duration"*  from TOML files.

## Example:
See [funks_test.go](https://github.com/uol/funks/blob/master/funks_test.go) for detailed examples.