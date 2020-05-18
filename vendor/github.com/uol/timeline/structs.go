package timeline

import (
	"time"

	"github.com/uol/hashing"
)

/**
* All common structs used by the timeline library.
* @author rnojiri
**/

// DataTransformerConf - flattener configuration
type DataTransformerConf struct {
	CycleDuration    time.Duration
	HashingAlgorithm hashing.Algorithm
	HashSize         int
	isSHAKE          bool
}
