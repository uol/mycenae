package persistence

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
)

func (backend *scylladb) statsQuery(function, keyspace string, operation constants.CRUDOperation, d time.Duration) {

	backend.timelineManager.FlattenMaxN(
		function,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		constants.StringsMetricScyllaQueryDuration,
		constants.StringsKeyspace, keyspace,
		constants.StringsOperation, operation,
	)

	backend.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricScyllaQuery,
		constants.StringsKeyspace, keyspace,
		constants.StringsOperation, operation,
	)
}

func (backend *scylladb) statsQueryError(function, keyspace string, operation constants.CRUDOperation) {

	backend.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricScyllaQueryError,
		constants.StringsKeyspace, keyspace,
		constants.StringsOperation, operation,
	)
}
