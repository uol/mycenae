package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/uol/gobol/logh"

	"github.com/gocql/gocql"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/loader"
	"github.com/uol/gobol/snitch"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/rest"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/telnet"
	"github.com/uol/mycenae/lib/telnetmgr"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/udp"
)

var (
	json   = jsoniter.ConfigCompatibleWithStandardLibrary
	logger *logh.ContextualLogger
)

func main() {

	fmt.Println("Starting Mycenae")

	//Parse of command line arguments.
	var confPath string
	var devMode bool

	flag.StringVar(&confPath, "config", "config.toml", "path to configuration file")
	flag.BoolVar(&devMode, "devMode", false, "enable/disable dev mode (all ttls are configured to one day)")
	flag.Parse()

	//Load conf file.
	settings := new(structs.Settings)

	err := loader.ConfToml(confPath, &settings)
	if err != nil {
		log.Fatalln("error loading config file: ", err)
	} else {
		fmt.Println("config file loaded: ", confPath)
	}

	logger = configureLogger(&settings.Logs)

	if devMode {
		if logh.InfoEnabled {
			logger.Info().Msg("DEV MODE IS ENABLED!")
		}
	}

	if settings.GarbageCollectorPercentage > 0 {
		debug.SetGCPercent(settings.GarbageCollectorPercentage)
		if logh.InfoEnabled {
			logger.Info().Msgf("using garbage collector percentage from configuration: %d%%", settings.GarbageCollectorPercentage)
		}
	}

	stats := createStatisticsService("stats", &settings.Stats)
	analyticsStats := createStatisticsService("analytics-stats", &settings.StatsAnalytic)
	timeseriesStats := createTimeseriesStatisticsService(stats, analyticsStats, settings)
	scyllaConn := createScyllaConnection(&settings.Cassandra)
	memcachedConn := createMemcachedConnection(&settings.Memcached, timeseriesStats)
	metadataStorage := createMetadataStorageService(&settings.MetadataSettings, timeseriesStats, memcachedConn)
	scyllaStorageService, keyspaceTTLMap := createScyllaStorageService(settings, devMode, timeseriesStats, scyllaConn, metadataStorage)
	keyspaceManager := createKeyspaceManager(settings, devMode, timeseriesStats, scyllaStorageService)
	keysetManager := createKeysetManager(settings, timeseriesStats, metadataStorage)
	collectorService := createCollectorService(settings, timeseriesStats, metadataStorage, scyllaConn, keysetManager, keyspaceTTLMap)
	plotService := createPlotService(settings, timeseriesStats, metadataStorage, scyllaConn, keyspaceTTLMap)
	udpServer := createUDPServer(&settings.UDPserver, collectorService, timeseriesStats)
	telnetManager := createTelnetManager(settings, collectorService, timeseriesStats)
	restServer := createRESTserver(settings, stats, plotService, collectorService, keyspaceManager, keysetManager, memcachedConn, telnetManager)

	if logh.InfoEnabled {
		logger.Info().Msg("mycenae started successfully")
	}

	stopChannel := make(chan os.Signal, 1)
	signal.Notify(stopChannel, os.Interrupt, syscall.SIGTERM)

	<-stopChannel

	if logh.InfoEnabled {
		logger.Info().Msg("stopping mycenae...")
	}

	if logh.InfoEnabled {
		logger.Info().Msg("stopping rest server")
	}

	restServer.Stop()

	if logh.InfoEnabled {
		logger.Info().Msg("rest server stopped")
	}

	if logh.InfoEnabled {
		logger.Info().Msg("stopping udp server")
	}

	udpServer.Stop()

	if logh.InfoEnabled {
		logger.Info().Msg("udp server stopped")
	}

	if logh.InfoEnabled {
		logger.Info().Msg("stopping telnet manager")
	}

	telnetManager.Shutdown()

	if logh.InfoEnabled {
		logger.Info().Msg("opentsdb telnet manager stopped")
	}

	if logh.InfoEnabled {
		logger.Info().Msg("stopping statistics service")
	}

	stats.Terminate()
	analyticsStats.Terminate()

	if logh.InfoEnabled {
		logger.Info().Msg("statistics service stopped")
	}

	if logh.InfoEnabled {
		logger.Info().Msg("stopping mycenae is done")
	}

	os.Exit(0)
}

// configureLogger - configures all loggers
func configureLogger(conf *structs.LoggerSettings) *logh.ContextualLogger {

	logh.ConfigureGlobalLogger(conf.Level, conf.Format)

	cl := logh.CreateContextualLogger(constants.StringsPKG, "main")

	if logh.InfoEnabled {
		cl.Info().Msg("log configured")
	}

	return cl
}

// createStatisticsService - creates the statistics service
func createStatisticsService(name string, conf *snitch.Settings) *snitch.Stats {

	stats, err := snitch.New(*conf)
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating statistics service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msgf("statistics service '%s' was created", name)
	}

	return stats
}

// createTimeseriesStatisticsService - create the timeseries statistics service
func createTimeseriesStatisticsService(stats, analitycsStats *snitch.Stats, settings *structs.Settings) *tsstats.StatsTS {

	tssts, err := tsstats.New(stats, analitycsStats, settings.Stats.Interval, settings.StatsAnalytic.Interval)
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating timeseries statistics service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("timeseries statistics service was created")
	}

	return tssts
}

// createScyllaConnection - creates the scylla DB connection
func createScyllaConnection(conf *cassandra.Settings) *gocql.Session {

	conn, err := cassandra.New(*conf)
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating scylla connection")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("scylla db connection was created")
	}

	return conn
}

// createMemcachedConnection - creates the memcached connection
func createMemcachedConnection(conf *memcached.Configuration, timeseriesStats *tsstats.StatsTS) *memcached.Memcached {

	mc, err := memcached.New(timeseriesStats, conf)
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating memcached connection")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("memcached connection was created")
	}

	return mc
}

// createMetadataStorageService - creates a new metadata storage
func createMetadataStorageService(conf *metadata.Settings, timeseriesStats *tsstats.StatsTS, memcachedConn *memcached.Memcached) *metadata.Storage {

	metaStorage, err := metadata.Create(
		conf,
		timeseriesStats,
		memcachedConn,
	)

	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating metadata storage service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("metadata storage service was created")
	}

	return metaStorage
}

// createScyllaStorageService - creates the scylla storage service
func createScyllaStorageService(conf *structs.Settings, devMode bool, timeseriesStats *tsstats.StatsTS, scyllaConn *gocql.Session, metadataStorage *metadata.Storage) (*persistence.Storage, map[int]string) {

	storage, err := persistence.NewStorage(
		conf.Cassandra.Keyspace,
		conf.Cassandra.Username,
		scyllaConn,
		metadataStorage,
		timeseriesStats,
		devMode,
		conf.DefaultTTL,
	)

	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating scylla storage service")
		}
		os.Exit(1)
	}

	jsonStr, _ := json.Marshal(conf.DefaultKeyspaces)

	if logh.InfoEnabled {
		logger.Info().Msgf("creating default keyspaces: %s", jsonStr)
	}

	keyspaceTTLMap := map[int]string{}
	for k, ttl := range conf.DefaultKeyspaces {
		gerr := storage.CreateKeyspace(k,
			conf.DefaultKeyspaceData.Datacenter,
			conf.DefaultKeyspaceData.Contact,
			conf.DefaultKeyspaceData.ReplicationFactor,
			ttl)
		keyspaceTTLMap[ttl] = k
		if gerr != nil && gerr.StatusCode() != http.StatusConflict {
			if logh.FatalEnabled {
				logger.Fatal().Err(err).Msgf("error creating keyspace '%s'", k)
			}
			os.Exit(1)
		}
	}

	if logh.InfoEnabled {
		logger.Info().Msg("scylla storage service was created")
	}

	return storage, keyspaceTTLMap
}

// createKeyspaceManager - creates the keyspace manager
func createKeyspaceManager(conf *structs.Settings, devMode bool, timeseriesStats *tsstats.StatsTS, scyllaStorageService *persistence.Storage) *keyspace.Keyspace {

	keyspaceManager := keyspace.New(
		timeseriesStats,
		scyllaStorageService,
		devMode,
		conf.DefaultTTL,
		conf.MaxAllowedTTL,
	)

	if logh.InfoEnabled {
		logger.Info().Msg("keyspace manager was created")
	}

	return keyspaceManager
}

// createKeysetManager - creates a new keyset manager
func createKeysetManager(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage) *keyset.KeySet {

	keySet := keyset.NewKeySet(metadataStorage, timeseriesStats)

	jsonStr, _ := json.Marshal(conf.DefaultKeysets)
	if logh.InfoEnabled {
		logger.Info().Msgf("creating default keysets: %s", jsonStr)
	}

	for _, v := range conf.DefaultKeysets {
		exists, err := metadataStorage.CheckKeySet(v)
		if err != nil {
			if logh.FatalEnabled {
				logger.Fatal().Err(err).Msgf("error checking keyset '%s' existence", v)
			}
			os.Exit(1)
		}
		if !exists {
			if logh.InfoEnabled {
				logger.Info().Msgf("creating default keyset '%s'", v)
			}
			err = keySet.CreateIndex(v)
			if err != nil {
				if logh.FatalEnabled {
					logger.Fatal().Err(err).Msgf("error creating keyset '%s'", v)
				}
				os.Exit(1)
			}
		}
	}

	if logh.InfoEnabled {
		logger.Info().Msg("keyset manager was created")
	}

	return keySet
}

// createCollectorService - creates a new collector service
func createCollectorService(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, keysetManager *keyset.KeySet, keyspaceTTLMap map[int]string) *collector.Collector {

	collector, err := collector.New(
		timeseriesStats,
		scyllaConn,
		metadataStorage,
		conf,
		keyspaceTTLMap,
		keysetManager,
	)

	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating collector service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("collector service was created")
	}

	return collector
}

// createPlotService - creates the plot service
func createPlotService(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, keyspaceTTLMap map[int]string) *plot.Plot {

	plotService, err := plot.New(
		scyllaConn,
		metadataStorage,
		conf.MaxTimeseries,
		conf.LogQueryTSthreshold,
		keyspaceTTLMap,
		conf.DefaultTTL,
		conf.DefaultPaginationSize,
		conf.MaxBytesOnQueryProcessing,
		timeseriesStats,
	)

	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating plot service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("plot service was created")
	}

	return plotService
}

// createUDPServer - creates the UDP server and starts it
func createUDPServer(conf *structs.SettingsUDP, collectorService *collector.Collector, stats *tsstats.StatsTS) *udp.UDPserver {

	udpServer := udp.New(*conf, collectorService, stats)
	udpServer.Start()

	if logh.InfoEnabled {
		logger.Info().Msg("udp server was created")
	}

	return udpServer
}

// createRESTserver - creates the REST server and starts it
func createRESTserver(conf *structs.Settings, stats *snitch.Stats, plotService *plot.Plot, collectorService *collector.Collector, keyspaceManager *keyspace.Keyspace, keysetManager *keyset.KeySet, memcachedConn *memcached.Memcached, telnetManager *telnetmgr.Manager) *rest.REST {

	restServer := rest.New(
		stats,
		plotService,
		keyspaceManager,
		memcachedConn,
		collectorService,
		conf.HTTPserver,
		conf.Probe.Threshold,
		keysetManager,
		telnetManager,
	)

	restServer.Start()

	if logh.InfoEnabled {
		logger.Info().Msg("rest server was created")
	}

	return restServer
}

// createTelnetManager - creates a new telnet manager
func createTelnetManager(conf *structs.Settings, collectorService *collector.Collector, stats *tsstats.StatsTS) *telnetmgr.Manager {

	telnetManager, err := telnetmgr.New(
		&conf.GlobalTelnetServerConfiguration,
		conf.HTTPserver.Port,
		collectorService,
		stats,
	)

	err = telnetManager.AddServer(&conf.NetdataServer, &conf.GlobalTelnetServerConfiguration, telnet.NewNetdataHandler(conf.NetdataServer.CacheDuration, collectorService, &conf.GlobalTelnetServerConfiguration))
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating telnet server 'netdata'")
		}
		os.Exit(1)
	}

	err = telnetManager.AddServer(&conf.TELNETserver, &conf.GlobalTelnetServerConfiguration, telnet.NewOpenTSDBHandler(collectorService, &conf.GlobalTelnetServerConfiguration))
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating telnet server 'telnet'")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("telnet manager was created")
	}

	return telnetManager
}
