package collector

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/utils"
)

const (
	metricProcTime            string = "points.processes.duration"
	metricMetaLost            string = "meta.lost"
	metricPointsReceived      string = "points.received"
	metricPointsReceivedError string = "points.received.error"
	metricTimeseriesCountNew  string = "timeseries.count.new"
	metricTimeseriesCountOld  string = "timeseries.count.old"
	metricScyllaRollbackError string = "scylla.rollback.error"
	metricDelayedMetric       string = "delayed.metrics"
)

func statsProcTime(ksid string, d time.Duration) {

	timelineManager.FlattenMaxN(
		constants.StringsEmpty,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		metricProcTime,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
	)
}

func statsLostMeta(ksid string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricMetaLost,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
	)
}

func statsInsertQuery(keyspace string, d time.Duration) {

	timelineManager.FlattenMaxN(
		constants.StringsEmpty,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		constants.StringsMetricScyllaQueryDuration,
		constants.StringsKeyspace, keyspace,
		constants.StringsOperation, constants.CRUDOperationInsert,
	)

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		constants.StringsMetricScyllaQuery,
		constants.StringsKeyspace, keyspace,
		constants.StringsOperation, constants.CRUDOperationInsert,
	)
}

func statsInsertQueryError(keyspace string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		constants.StringsMetricScyllaQueryError,
		constants.StringsKeyspace, utils.ValidateExpectedValue(keyspace),
		constants.StringsOperation, constants.CRUDOperationInsert,
	)
}

func statsInsertRollback(keyspace string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricScyllaRollbackError,
		constants.StringsKeyspace, utils.ValidateExpectedValue(keyspace),
		constants.StringsOperation, constants.CRUDOperationInsert,
	)
}

func statsPoints(ksid, metaType string, sourceType *constants.SourceType, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricPointsReceived,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsProtocol, sourceType.Name,
		constants.StringsType, metaType,
	)
}

func statsPointsError(ksid, metaType string, sourceType *constants.SourceType, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricPointsReceivedError,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsProtocol, sourceType.Name,
		constants.StringsType, metaType,
	)
}

func statsCountNewTimeseries(ksid, metaType string, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricTimeseriesCountNew,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsType, metaType,
	)
}

func statsCountOldTimeseries(ksid, metaType string, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricTimeseriesCountOld,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsType, metaType,
	)
}

func statsNetworkIP(ip, source string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		constants.StringsMetricNetworkIP,
		constants.StringsIP, ip,
		constants.StringsSource, source)
}

func statsDelayedMetrics(ksid string, pastTime int64) {

	timelineManager.FlattenMaxN(
		constants.StringsEmpty,
		float64(pastTime),
		metricDelayedMetric,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
	)
}
