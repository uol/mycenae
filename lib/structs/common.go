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

type SettingsUDP struct {
	Port       string
	ReadBuffer int
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
}
