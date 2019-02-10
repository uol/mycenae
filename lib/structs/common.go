package structs

import (
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	"go.uber.org/zap"
)

type LogSetting struct {
	Level  string
	Prefix string
}

type TsLog struct {
	General *zap.Logger
	Stats   *zap.Logger
}

type SettingsHTTP struct {
	Path string
	Port string
	Bind string
}

type SettingsTelnet struct {
	Port           int
	Host           string
	OnErrorTimeout int
	MaxBufferSize  int64
}

type SettingsUDP struct {
	Port       string
	ReadBuffer int
}

type NetdataMetricReplacement struct {
	LookForPropertyName  string
	LookForPropertyValue string
	PropertyAsNewMetric  string
	NewTagName           string
	NewTagValue          string
}

type Settings struct {
	MaxTimeseries         int
	LogQueryTSthreshold   int
	MaxConcurrentPoints   int
	MaxConcurrentBulks    int
	MaxMetaBulkSize       int
	MetaBufferSize        int
	DefaultPaginationSize int
	MetaSaveInterval      string
	HostNameCleanerRegex  string
	HTTPserver            SettingsHTTP
	UDPserver             SettingsUDP
	TELNETserver          SettingsTelnet
	NetdataServer         SettingsTelnet
	DefaultTTL            int
	MaxAllowedTTL         int
	DefaultKeysets        []string
	BlacklistedKeysets    []string
	DefaultKeyspaceData   keyspace.Config
	DefaultKeyspaces      map[string]int
	Cassandra             cassandra.Settings
	Memcached             memcached.Configuration
	AllowCORS             bool
	Logs                  struct {
		Environment string
		General     LogSetting
		Stats       LogSetting
	}
	Stats            snitch.Settings
	StatsAnalytic    snitch.Settings
	MetadataSettings metadata.Settings
	Probe            struct {
		Threshold float64
	}
	NetdataHandlerReplacements []NetdataMetricReplacement
}
