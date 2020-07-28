package structs

import (
	"github.com/uol/funks"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/logh"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	tlmanager "github.com/uol/timelinemanager"
)

type SettingsHTTP struct {
	Port              int
	Bind              string
	EnableProfiling   bool
	ForceErrorAsDebug bool
	AllowCORS         bool
}

type SettingsUDP struct {
	Port             int
	SendStatsTimeout string
	ReadBuffer       int
}

type LoggerSettings struct {
	Level  logh.Level
	Format logh.Format
}

// ValidationConfiguration - validation configurations
type ValidationConfiguration struct {
	MaxTextValueSize int
	MaxNumTags       int
	PropertyRegexp   string
	KeysetNameRegexp string
	DefaultTTL       int
	MaxPropertySize  int
}

// TelnetManagerConfiguration - the main and shared configuration for all telnet servers
type TelnetManagerConfiguration struct {
	MaxTelnetConnections              uint32
	MaxUnbalancedTelnetConnsPerNode   uint32
	TelnetConnsBalanceCheckInterval   funks.Duration
	MaxWaitForDropTelnetConnsInterval funks.Duration
	NodeToNodeRequestTimeout          funks.Duration
	MaxWaitForOtherNodeConnsBalancing funks.Duration
	ConnectionCloseChannelSize        int
	Nodes                             []string
	SendStatsTimeout                  funks.Duration
}

// TelnetServerConfiguration - the telnet server/handler configuration
type TelnetServerConfiguration struct {
	Port                           int
	Host                           string
	MaxBufferSize                  int64
	CacheDuration                  string
	MaxIdleConnectionTimeout       funks.Duration
	ServerName                     string
	SilenceLogs                    bool
	RemoveMultipleConnsRestriction bool
	MultipleConnsAllowedHosts      []string
}

type Settings struct {
	MaxTimeseries                      int
	LogQueryTSthreshold                int
	MaxConcurrentPoints                int
	DefaultPaginationSize              int
	MaxBytesOnQueryProcessing          uint32
	UnlimitedQueryBytesKeysetWhiteList []string
	SilencePointValidationErrors       bool
	GarbageCollectorPercentage         int
	TSIDKeySize                        int
	DelayedMetricsThreshold            int64
	TelnetManagerConfiguration         TelnetManagerConfiguration
	HTTPserver                         SettingsHTTP
	UDPserver                          SettingsUDP
	TELNETserver                       []TelnetServerConfiguration
	NetdataServer                      []TelnetServerConfiguration
	MaxAllowedTTL                      int
	DefaultKeysets                     []string
	BlacklistedKeysets                 []string
	DefaultKeyspaceData                keyspace.Config
	DefaultKeyspaces                   map[string]int
	EnableAutoKeyspaceCreation         bool
	Cassandra                          cassandra.Settings
	Memcached                          memcached.Configuration
	Logs                               LoggerSettings
	Stats                              tlmanager.Configuration
	MetadataSettings                   metadata.Settings
	Validation                         ValidationConfiguration
}
