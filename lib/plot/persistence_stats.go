package plot

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
)

const (
	metricScyllaQueryBytes   string = "scylla.query.bytes"
	metricScyllaQueryMaxRows string = "scylla.query.max.rows"
)

type keyspaceType string

const (
	typeNumber keyspaceType = "number"
	typeText   keyspaceType = "text"
)

const (
	scyllaSelect string = "select"
)

func (persist *persistence) statsQueryError(function, keyset, keyspace string, ksType keyspaceType) {

	persist.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricScyllaQueryError,
		constants.StringsTargetKSID, keyset,
		constants.StringsKeyspace, keyspace,
		constants.StringsType, ksType,
	)
}

func (persist *persistence) statsSelect(function, keyset, keyspace string, ksType keyspaceType, d time.Duration, countRows int) {

	persist.timelineManager.FlattenCountIncN(
		function,
		constants.StringsMetricScyllaQuery,
		constants.StringsTargetKSID, keyset,
		constants.StringsKeyspace, keyspace,
		constants.StringsType, ksType,
	)

	persist.timelineManager.FlattenMaxN(
		function,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		constants.StringsMetricScyllaQueryDuration,
		constants.StringsTargetKSID, keyset,
		constants.StringsKeyspace, keyspace,
		constants.StringsType, ksType,
	)

	persist.timelineManager.FlattenMaxN(
		function,
		float64(countRows),
		metricScyllaQueryMaxRows,
		constants.StringsTargetKSID, keyset,
		constants.StringsKeyspace, keyspace,
		constants.StringsType, ksType,
	)
}

func (persist *persistence) statsQueryBytes(function, keyset, keyspace string, ksType keyspaceType, value float64) {

	persist.timelineManager.FlattenMaxN(
		function,
		value,
		metricScyllaQueryBytes,
		constants.StringsTargetKSID, keyset,
		constants.StringsKeyspace, keyspace,
		constants.StringsType, ksType,
	)
}
