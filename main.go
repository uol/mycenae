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

	"github.com/uol/logh"

	"github.com/gocql/gocql"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/loader"

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
	"github.com/uol/mycenae/lib/udp"
	"github.com/uol/mycenae/lib/validation"
	tlmanager "github.com/uol/timelinemanager"
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

	timelineManager := createTimelineManager(&settings.Stats)
	scyllaConn := createScyllaConnection(&settings.Cassandra)
	memcachedConn := createMemcachedConnection(&settings.Memcached, timelineManager)
	metadataStorage := createMetadataStorageService(&settings.MetadataSettings, timelineManager, memcachedConn)
	scyllaStorageService, keyspaceTTLMap := createScyllaStorageService(settings, devMode, timelineManager, scyllaConn, metadataStorage)
	validationService := createValidation(settings, metadataStorage, keyspaceTTLMap, timelineManager)
	collectorService := createCollectorService(settings, timelineManager, metadataStorage, scyllaConn, validationService, keyspaceTTLMap)
	telnetManager := createTelnetManager(settings, collectorService, timelineManager, validationService)

	err = timelineManager.Start()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Err(err).Msg("error starting timeline manager")
		}
		os.Exit(1)
	}

	keyspaceManager := createKeyspaceManager(settings, devMode, timelineManager, scyllaStorageService)
	keysetManager := createKeysetManager(settings, metadataStorage)
	plotService := createPlotService(settings, timelineManager, metadataStorage, scyllaConn, keyspaceTTLMap)
	udpServer := createUDPServer(&settings.UDPserver, collectorService, timelineManager)
	restServer := createRESTserver(settings, timelineManager, plotService, collectorService, keyspaceManager, keysetManager, memcachedConn, telnetManager)

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

	timelineManager.Shutdown()

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

// createTimelineManager - creates the timeline manager
func createTimelineManager(settings *tlmanager.Configuration) *tlmanager.Instance {

	if logh.DebugEnabled {
		logger.Debug().Msgf("%+v", *settings)
	}

	tm, err := tlmanager.New(settings)
	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating timeline manager")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("timeline manager was created")
	}

	return tm
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
func createMemcachedConnection(conf *memcached.Configuration, timelineManager *tlmanager.Instance) *memcached.Memcached {

	mc, err := memcached.New(timelineManager, conf)
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
func createMetadataStorageService(conf *metadata.Settings, timelineManager *tlmanager.Instance, memcachedConn *memcached.Memcached) *metadata.Storage {

	metaStorage, err := metadata.Create(
		conf,
		timelineManager,
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
func createScyllaStorageService(conf *structs.Settings, devMode bool, timelineManager *tlmanager.Instance, scyllaConn *gocql.Session, metadataStorage *metadata.Storage) (*persistence.Storage, map[int]string) {

	storage, err := persistence.NewStorage(
		conf.Cassandra.Keyspace,
		conf.Cassandra.Username,
		scyllaConn,
		metadataStorage,
		timelineManager,
		devMode,
		conf.Validation.DefaultTTL,
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
		if conf.EnableAutoKeyspaceCreation {
			gerr := storage.CreateKeyspace(k,
				conf.DefaultKeyspaceData.Datacenter,
				conf.DefaultKeyspaceData.Contact,
				conf.DefaultKeyspaceData.ReplicationFactor,
				ttl)

			if gerr != nil && gerr.StatusCode() != http.StatusConflict {
				if logh.FatalEnabled {
					logger.Fatal().Err(err).Msgf("error creating keyspace '%s'", k)
				}
				os.Exit(1)
			}
		}

		keyspaceTTLMap[ttl] = k
	}

	if logh.InfoEnabled {
		logger.Info().Msg("scylla storage service was created")
	}

	return storage, keyspaceTTLMap
}

// createKeyspaceManager - creates the keyspace manager
func createKeyspaceManager(conf *structs.Settings, devMode bool, timelineManager *tlmanager.Instance, scyllaStorageService *persistence.Storage) *keyspace.Keyspace {

	keyspaceManager := keyspace.New(
		timelineManager,
		scyllaStorageService,
		devMode,
		conf.Validation.DefaultTTL,
		conf.MaxAllowedTTL,
	)

	if logh.InfoEnabled {
		logger.Info().Msg("keyspace manager was created")
	}

	return keyspaceManager
}

// createKeysetManager - creates a new keyset manager
func createKeysetManager(conf *structs.Settings, metadataStorage *metadata.Storage) *keyset.Manager {

	keyset := keyset.New(metadataStorage, conf.Validation.KeysetNameRegexp)

	jsonStr, _ := json.Marshal(conf.DefaultKeysets)
	if logh.InfoEnabled {
		logger.Info().Msgf("creating default keysets: %s", jsonStr)
	}

	for _, v := range conf.DefaultKeysets {
		exists := metadataStorage.CheckKeyset(v)
		if !exists {
			if logh.InfoEnabled {
				logger.Info().Msgf("creating default keyset '%s'", v)
			}
			err := keyset.Create(v)
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

	return keyset
}

// createCollectorService - creates a new collector service
func createCollectorService(conf *structs.Settings, timelineManager *tlmanager.Instance, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, validationService *validation.Service, keyspaceTTLMap map[int]string) *collector.Collector {

	collector, err := collector.New(
		timelineManager,
		scyllaConn,
		metadataStorage,
		conf,
		keyspaceTTLMap,
		validationService,
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
func createPlotService(conf *structs.Settings, timelineManager *tlmanager.Instance, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, keyspaceTTLMap map[int]string) *plot.Plot {

	plotService, err := plot.New(
		scyllaConn,
		metadataStorage,
		conf.MaxTimeseries,
		conf.LogQueryTSthreshold,
		keyspaceTTLMap,
		conf.Validation.DefaultTTL,
		conf.DefaultPaginationSize,
		conf.MaxBytesOnQueryProcessing,
		conf.UnlimitedQueryBytesKeysetWhiteList,
		timelineManager,
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
func createUDPServer(conf *structs.SettingsUDP, collectorService *collector.Collector, timelineManager *tlmanager.Instance) *udp.UDPserver {

	udpServer := udp.New(*conf, collectorService, timelineManager)
	udpServer.Start()

	if logh.InfoEnabled {
		logger.Info().Msg("udp server was created")
	}

	return udpServer
}

// createRESTserver - creates the REST server and starts it
func createRESTserver(conf *structs.Settings, timelineManager *tlmanager.Instance, plotService *plot.Plot, collectorService *collector.Collector, keyspaceManager *keyspace.Keyspace, keysetManager *keyset.Manager, memcachedConn *memcached.Memcached, telnetManager *telnetmgr.Manager) *rest.REST {

	restServer := rest.New(
		timelineManager,
		plotService,
		keyspaceManager,
		memcachedConn,
		collectorService,
		conf.HTTPserver,
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
func createTelnetManager(conf *structs.Settings, collectorService *collector.Collector, timelineManager *tlmanager.Instance, validationService *validation.Service) *telnetmgr.Manager {

	telnetManager, err := telnetmgr.New(
		&conf.TelnetManagerConfiguration,
		conf.HTTPserver.Port,
		collectorService,
		timelineManager,
	)

	for i := 0; i < len(conf.NetdataServer); i++ {
		err = telnetManager.AddServer(&conf.NetdataServer[i], &conf.TelnetManagerConfiguration, telnet.NewNetdataHandler(conf.NetdataServer[i].CacheDuration, collectorService, &conf.NetdataServer[i], validationService))
		if err != nil {
			if logh.FatalEnabled {
				logger.Fatal().Err(err).Msg("error creating telnet server 'netdata'")
			}
			os.Exit(1)
		}
	}

	for i := 0; i < len(conf.TELNETserver); i++ {
		err = telnetManager.AddServer(&conf.TELNETserver[i], &conf.TelnetManagerConfiguration, telnet.NewOpenTSDBHandler(collectorService, &conf.TELNETserver[i], validationService))
		if err != nil {
			if logh.FatalEnabled {
				logger.Fatal().Err(err).Msg("error creating telnet server 'telnet'")
			}
			os.Exit(1)
		}
	}

	if logh.InfoEnabled {
		logger.Info().Msg("telnet manager was created")
	}

	return telnetManager
}

// createValidation - creates a new validation service
func createValidation(conf *structs.Settings, metadataStorage *metadata.Storage, keyspaceTTLMap map[int]string, timelineManager *tlmanager.Instance) *validation.Service {

	service, err := validation.New(
		&conf.Validation,
		metadataStorage,
		keyspaceTTLMap,
		timelineManager,
	)

	if err != nil {
		if logh.FatalEnabled {
			logger.Fatal().Err(err).Msg("error creating validation service")
		}
		os.Exit(1)
	}

	if logh.InfoEnabled {
		logger.Info().Msg("validation service was created")
	}

	return service
}
