package timeline

import (
	"github.com/uol/funks"
	"github.com/uol/hashing"
)

/**
* All common structs used by the timeline library.
* @author rnojiri
**/

// DataTransformerConf - flattener configuration
type DataTransformerConf struct {
	CycleDuration    funks.Duration
	HashingAlgorithm hashing.Algorithm
	HashSize         int
	isSHAKE          bool
	Name             string
}
