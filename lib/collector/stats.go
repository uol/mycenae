package collector

import (
	"time"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/utils"
)

const (
	metricProcTime            string = "points.processes_time"
	metricMetaLost            string = "meta.lost"
	metricPointsReceived      string = "points.received"
	metricPointsReceivedError string = "points.received.error"
	metricTimeseriesCountNew  string = "timeseries.count.new"
	metricTimeseriesCountOld  string = "timeseries.count.old"
	metricScyllaRollbackError string = "scylla.rollback.error"
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

func statsInsertQuery(keyspace, columnFamily string, d time.Duration) {

	timelineManager.FlattenMaxN(
		constants.StringsEmpty,
		float64(d.Nanoseconds())/float64(time.Millisecond),
		constants.StringsMetricScyllaQueryDuration,
		constants.StringsKeyspace, keyspace,
		constants.StringColumnFamily, columnFamily,
		constants.StringsOperation, constants.StringsInsert,
	)

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		constants.StringsMetricScyllaQuery,
		constants.StringsKeyspace, keyspace,
		constants.StringColumnFamily, columnFamily,
		constants.StringsOperation, constants.StringsInsert,
	)
}

func statsInsertQueryError(keyspace, columnFamily string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		constants.StringsMetricScyllaQueryError,
		constants.StringsKeyspace, utils.ValidateExpectedValue(keyspace),
		constants.StringColumnFamily, columnFamily,
		constants.StringsOperation, constants.StringsInsert,
	)
}

func statsInsertRollback(keyspace, columnFamily string) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricScyllaRollbackError,
		constants.StringsKeyspace, utils.ValidateExpectedValue(keyspace),
		constants.StringColumnFamily, columnFamily,
		constants.StringsOperation, constants.StringsInsert,
	)
}

func statsPoints(ksid, metaType, protocol string, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricPointsReceived,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsProtocol, protocol,
		constants.StringsType, metaType,
	)
}

func statsPointsError(ksid, metaType, protocol string, ttl int) {

	timelineManager.FlattenCountIncN(
		constants.StringsEmpty,
		metricPointsReceivedError,
		constants.StringsTargetKSID, utils.ValidateExpectedValue(ksid),
		constants.StringsTargetTTL, ttl,
		constants.StringsProtocol, protocol,
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
		constants.StringsMetricNetworkIP,
		constants.StringsIP, ip,
		constants.StringsSource, source)
}
