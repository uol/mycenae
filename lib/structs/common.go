package structs

import (
	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/saw"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
)

type TsLog struct {
	General *logrus.Logger
	Stats   *logrus.Logger
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
	MaxTimeseries           int
	MaxConcurrentTimeseries int
	MaxConcurrentReads      int
	LogQueryTSthreshold     int
	MaxConcurrentPoints     int
	MaxConcurrentBulks      int
	MaxMetaBulkSize         int
	MetaBufferSize          int
	MetaSaveInterval        string
	HTTPserver              SettingsHTTP
	UDPserver               SettingsUDP
	UDPserverV2             SettingsUDP
	DefaultTTL				uint8
	MaxAllowedTTL			int
	DefaultKeysets			[]string
	DefaultKeyspaceData     keyspace.Config
	DefaultKeyspaces        map[string]uint8
	Cassandra               cassandra.Settings
	Logs                    struct {
		General saw.Settings
		Stats   saw.Settings
	}
	Stats         snitch.Settings
	ElasticSearch struct {
		Cluster rubber.Settings
		Index   string
	}
	Probe struct {
		Threshold float64
	}
	Memcached memcached.Configuration
}
