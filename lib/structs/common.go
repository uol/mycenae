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

type Loggers struct {
	General *zap.Logger
	Stats   *zap.Logger
}

type SettingsHTTP struct {
	Path string
	Port string
	Bind string
}

type SettingsTelnet struct {
	Port                     int
	Host                     string
	OnErrorTimeout           string
	SendStatsTimeout         string
	MaxBufferSize            int64
	CacheDuration            string
	MaxIdleConnectionTimeout string
}

type SettingsUDP struct {
	Port             string
	SendStatsTimeout string
	ReadBuffer       int
}

type LoggerSettings struct {
	Environment string
	General     LogSetting
	Stats       LogSetting
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
	RawNetdataServer      SettingsTelnet
	DefaultTTL            int
	MaxAllowedTTL         int
	DefaultKeysets        []string
	BlacklistedKeysets    []string
	DefaultKeyspaceData   keyspace.Config
	DefaultKeyspaces      map[string]int
	Cassandra             cassandra.Settings
	Memcached             memcached.Configuration
	AllowCORS             bool
	Logs                  LoggerSettings
	Stats                 snitch.Settings
	StatsAnalytic         snitch.Settings
	MetadataSettings      metadata.Settings
	Probe                 struct {
		Threshold float64
	}
}
