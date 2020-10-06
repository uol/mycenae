package constants

const (
	// StringsEmpty - a empty space
	StringsEmpty string = ""

	// StringsComma - a comma
	StringsComma string = ","

	// StringsWhitespace - a white space
	StringsWhitespace string = " "

	// StringsPKG - the package abbreviation
	StringsPKG string = "pkg"

	// StringsFunc - the function abbreviation
	StringsFunc string = "func"

	// StringsMetric - metric word
	StringsMetric string = "metric"

	// StringsTimestamp - timestamp word
	StringsTimestamp string = "timestamp"

	// StringsValue - value word
	StringsValue string = "value"

	// StringsTags - tags word
	StringsTags string = "tags"

	// StringsText - text word
	StringsText string = "text"

	// StringsKeyset - keyset word
	StringsKeyset string = "keyset"

	// StringsTTL - ttl word
	StringsTTL string = "ttl"

	// StringsKSID - ksid word
	StringsKSID string = "ksid"

	// StringsTargetTTL - target ttl tag
	StringsTargetTTL string = "target_ttl"

	// StringsTargetKSID - target ksid tag
	StringsTargetKSID string = "target_ksid"

	// StringsKeyspace - target keyspace tag
	StringsKeyspace string = "keyspace"

	// StringsProtocol - protocol tag
	StringsProtocol string = "protocol"

	// StringsBar - bar character
	StringsBar string = "/"

	// StringsHost - host tag
	StringsHost string = "host"

	// StringsIP - ip tag
	StringsIP string = "ip"

	// StringsSource - source tag
	StringsSource string = "source"

	// StringsOperation - operation tag
	StringsOperation string = "operation"

	// StringsType - type tag
	StringsType string = "type"

	// StringsHTTP - "http" word
	StringsHTTP string = "http"

	// StringsMetricScyllaQuery - metric name for scylla query
	StringsMetricScyllaQuery string = "scylla.query"

	// StringsMetricScyllaQueryDuration - metric name for scylla query duration
	StringsMetricScyllaQueryDuration string = "scylla.query.duration"

	// StringsMetricScyllaQueryError - metric name for scylla query errors
	StringsMetricScyllaQueryError string = "scylla.query.error"

	// StringsMetricNetworkIP - metric name for network ip
	StringsMetricNetworkIP string = "network.ip"

	// StringsAll - "all" word
	StringsAll string = "all"

	// StringsUDP - "udp" word
	StringsUDP string = "udp"

	// StringsMetricNetworkConnection - network connection metric
	StringsMetricNetworkConnection string = "network.connection"

	// StringsUnknown - "unknown" word
	StringsUnknown string = "unknown"
)

// CRUDOperation - defines a database CRUD operation
type CRUDOperation string

const (
	// CRUDOperationCreate - CRUD operation
	CRUDOperationCreate CRUDOperation = "create"

	// CRUDOperationInsert - CRUD operation
	CRUDOperationInsert CRUDOperation = "insert"

	// CRUDOperationSelect - CRUD operation
	CRUDOperationSelect CRUDOperation = "select"

	// CRUDOperationDelete - CRUD operation
	CRUDOperationDelete CRUDOperation = "delete"

	// CRUDOperationUpdate - CRUD operation
	CRUDOperationUpdate CRUDOperation = "update"

	// CRUDOperationDrop - CRUD operation
	CRUDOperationDrop CRUDOperation = "drop"
)

// ClusteringOrder - defines the table clustering order
type ClusteringOrder string

const (
	// ClusteringOrderASC - clustering order
	ClusteringOrderASC ClusteringOrder = "ASC"

	// ClusteringOrderDESC - clustering order
	ClusteringOrderDESC ClusteringOrder = "DESC"
)
