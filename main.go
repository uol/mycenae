package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap/zapcore"

	"github.com/gocql/gocql"
	jsoniter "github.com/json-iterator/go"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/loader"
	"github.com/uol/gobol/saw"
	"github.com/uol/gobol/snitch"
	"go.uber.org/zap"

	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/keyset"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/rest"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/telnet"
	"github.com/uol/mycenae/lib/telnetsrv"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/udp"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "main"),
	}

	loggers := createLoggers(&settings.Logs)

	if devMode {
		loggers.General.Info("DEV MODE IS ENABLED!", lf...)
	}

	var numActiveTelnetConnections uint32

	stats := createStatisticsService("stats", &settings.Stats, loggers.Stats)
	analyticsStats := createStatisticsService("analytics-stats", &settings.StatsAnalytic, loggers.Stats)
	timeseriesStats := createTimeseriesStatisticsService(stats, analyticsStats, settings, loggers.General)
	scyllaConn := createScyllaConnection(&settings.Cassandra, loggers.General)
	memcachedConn := createMemcachedConnection(&settings.Memcached, timeseriesStats, loggers.General)
	metadataStorage := createMetadataStorageService(&settings.MetadataSettings, timeseriesStats, memcachedConn, loggers.General)
	scyllaStorageService, keyspaceTTLMap := createScyllaStorageService(settings, devMode, timeseriesStats, scyllaConn, metadataStorage, loggers.General)
	keyspaceManager := createKeyspaceManager(settings, devMode, timeseriesStats, scyllaStorageService, loggers.General)
	keysetManager := createKeysetManager(settings, timeseriesStats, metadataStorage, loggers.General)
	collectorService := createCollectorService(settings, timeseriesStats, metadataStorage, scyllaConn, keysetManager, keyspaceTTLMap, loggers)
	plotService := createPlotService(settings, timeseriesStats, metadataStorage, scyllaConn, keyspaceTTLMap, loggers.General)
	udpServer := createUDPServer(&settings.UDPserver, collectorService, timeseriesStats, loggers.General)
	restServer := createRESTserver(settings, stats, plotService, collectorService, keyspaceManager, keysetManager, memcachedConn, loggers)
	telnetServer := createTelnetServer(&settings.TELNETserver, "opentsdb telnet server", telnet.NewOpenTSDBHandler(collectorService, loggers.General), collectorService, timeseriesStats, &numActiveTelnetConnections, settings.MaxTelnetConnections, loggers.General)
	netdataServer := createTelnetServer(&settings.NetdataServer, "netdata telnet server", telnet.NewNetdataHandler(settings.NetdataServer.CacheDuration, collectorService, loggers.General), collectorService, timeseriesStats, &numActiveTelnetConnections, settings.MaxTelnetConnections, loggers.General)

	loggers.General.Info("mycenae started successfully", lf...)

	stopChannel := make(chan os.Signal, 1)
	signal.Notify(stopChannel, os.Interrupt, syscall.SIGTERM)

	<-stopChannel

	loggers.General.Info("stopping mycenae...", lf...)

	loggers.General.Info("stopping rest server", lf...)
	restServer.Stop()
	loggers.General.Info("rest server stopped", lf...)

	loggers.General.Info("stopping udp server", lf...)
	udpServer.Stop()
	loggers.General.Info("udp server stopped", lf...)

	loggers.General.Info("stopping opentsdb telnet server", lf...)
	telnetServer.Shutdown()
	loggers.General.Info("opentsdb telnet server stopped", lf...)

	loggers.General.Info("stopping netdata telnet server", lf...)
	netdataServer.Shutdown()
	loggers.General.Info("netdata telnet server stopped", lf...)

	loggers.General.Info("stopping statistics service", lf...)
	stats.Terminate()
	analyticsStats.Terminate()
	loggers.General.Info("statistics service stopped", lf...)

	loggers.General.Info("stopping mycenae is done")

	os.Exit(0)
}

// createLoggers - create all loggers
func createLoggers(conf *structs.LoggerSettings) *structs.Loggers {

	var err error
	loggers := &structs.Loggers{}

	loggers.General, err = saw.New(conf.General.Level, conf.Environment)
	if err != nil {
		log.Fatalln("error creating logger: ", err)
		os.Exit(1)
	}
	loggers.General = loggers.General.With(zap.String("type", conf.General.Prefix))

	loggers.Stats, err = saw.New(conf.Stats.Level, conf.Environment)
	if err != nil {
		log.Fatalln("error creating logger: ", err)
		os.Exit(1)
	}
	loggers.Stats = loggers.Stats.With(zap.String("type", conf.Stats.Prefix))

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createLoggers"),
	}

	loggers.General.Info("loggers created", lf...)

	return loggers
}

// createStatisticsService - creates the statistics service
func createStatisticsService(name string, conf *snitch.Settings, logger *zap.Logger) *snitch.Stats {

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createStatisticsService"),
	}

	stats, err := snitch.New(logger, *conf)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating statistics service: %s", err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info(fmt.Sprintf("statistics service '%s' was created", name), lf...)

	return stats
}

// createTimeseriesStatisticsService - create the timeseries statistics service
func createTimeseriesStatisticsService(stats, analitycsStats *snitch.Stats, settings *structs.Settings, logger *zap.Logger) *tsstats.StatsTS {

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createTimeseriesStatisticsService"),
	}

	tssts, err := tsstats.New(logger, stats, analitycsStats, settings.Stats.Interval, settings.StatsAnalytic.Interval)
	if err != nil {
		logger.Error(err.Error(), lf...)
		os.Exit(1)
	}

	logger.Info("timeseries statistics service was created", lf...)

	return tssts
}

// createScyllaConnection - creates the scylla DB connection
func createScyllaConnection(conf *cassandra.Settings, logger *zap.Logger) *gocql.Session {

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createScyllaConnection"),
	}

	conn, err := cassandra.New(*conf)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating scylla connection: %s", err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info("scylla db connection was created", lf...)

	return conn
}

// createMemcachedConnection - creates the memcached connection
func createMemcachedConnection(conf *memcached.Configuration, timeseriesStats *tsstats.StatsTS, logger *zap.Logger) *memcached.Memcached {

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createMemcachedConnection"),
	}

	mc, err := memcached.New(timeseriesStats, conf)
	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating memcached connection: %s", err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info("memcached connection was created", lf...)

	return mc
}

// createMetadataStorageService - creates a new metadata storage
func createMetadataStorageService(conf *metadata.Settings, timeseriesStats *tsstats.StatsTS, memcachedConn *memcached.Memcached, logger *zap.Logger) *metadata.Storage {

	metaStorage, err := metadata.Create(
		conf,
		logger,
		timeseriesStats,
		memcachedConn,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createMetadataStorageService"),
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating metadata storage service: %s", err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info("metadata storage service was created", lf...)

	return metaStorage
}

// createScyllaStorageService - creates the scylla storage service
func createScyllaStorageService(conf *structs.Settings, devMode bool, timeseriesStats *tsstats.StatsTS, scyllaConn *gocql.Session, metadataStorage *metadata.Storage, logger *zap.Logger) (*persistence.Storage, map[int]string) {

	storage, err := persistence.NewStorage(
		conf.Cassandra.Keyspace,
		conf.Cassandra.Username,
		logger,
		scyllaConn,
		metadataStorage,
		timeseriesStats,
		devMode,
		conf.DefaultTTL,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createScyllaStorageService"),
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating scylla storage service: %s", err.Error()), lf...)
		os.Exit(1)
	}

	jsonStr, _ := json.Marshal(conf.DefaultKeyspaces)

	logger.Info(fmt.Sprintf("creating default keyspaces: %s", jsonStr), lf...)

	keyspaceTTLMap := map[int]string{}
	for k, ttl := range conf.DefaultKeyspaces {
		gerr := storage.CreateKeyspace(k,
			conf.DefaultKeyspaceData.Datacenter,
			conf.DefaultKeyspaceData.Contact,
			conf.DefaultKeyspaceData.ReplicationFactor,
			ttl)
		keyspaceTTLMap[ttl] = k
		if gerr != nil && gerr.StatusCode() != http.StatusConflict {
			logger.Fatal(fmt.Sprintf("error creating keyspace '%s': %s", k, gerr.Message()), lf...)
			os.Exit(1)
		}
	}

	logger.Info("scylla storage service was created", lf...)

	return storage, keyspaceTTLMap
}

// createKeyspaceManager - creates the keyspace manager
func createKeyspaceManager(conf *structs.Settings, devMode bool, timeseriesStats *tsstats.StatsTS, scyllaStorageService *persistence.Storage, logger *zap.Logger) *keyspace.Keyspace {

	keyspaceManager := keyspace.New(
		timeseriesStats,
		scyllaStorageService,
		devMode,
		conf.DefaultTTL,
		conf.MaxAllowedTTL,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createKeyspaceManager"),
	}

	logger.Info("keyspace manager was created", lf...)

	return keyspaceManager
}

// createKeysetManager - creates a new keyset manager
func createKeysetManager(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage, logger *zap.Logger) *keyset.KeySet {

	keySet := keyset.NewKeySet(metadataStorage, timeseriesStats)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createKeysetManager"),
	}

	jsonStr, _ := json.Marshal(conf.DefaultKeysets)
	logger.Info(fmt.Sprintf("creating default keysets: %s", jsonStr), lf...)
	for _, v := range conf.DefaultKeysets {
		exists, err := metadataStorage.CheckKeySet(v)
		if err != nil {
			logger.Fatal(fmt.Sprintf("error checking keyset '%s' existence: %s", v, err.Error()), lf...)
			os.Exit(1)
		}
		if !exists {
			logger.Info(fmt.Sprintf("creating default keyset '%s'", v), lf...)
			err = keySet.CreateIndex(v)
			if err != nil {
				logger.Fatal(fmt.Sprintf("error creating keyset '%s': %s", v, err.Error()), lf...)
				os.Exit(1)
			}
		}
	}

	logger.Info("keyset manager was created", lf...)

	return keySet
}

// createCollectorService - creates a new collector service
func createCollectorService(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, keysetManager *keyset.KeySet, keyspaceTTLMap map[int]string, loggers *structs.Loggers) *collector.Collector {

	collector, err := collector.New(
		loggers,
		timeseriesStats,
		scyllaConn,
		metadataStorage,
		conf,
		keyspaceTTLMap,
		keysetManager,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createCollectorService"),
	}

	if err != nil {
		loggers.General.Fatal(fmt.Sprintf("error creating collector service: %s", err.Error()), lf...)
		os.Exit(1)
	}

	loggers.General.Info("collector service was created", lf...)

	return collector
}

// createPlotService - creates the plot service
func createPlotService(conf *structs.Settings, timeseriesStats *tsstats.StatsTS, metadataStorage *metadata.Storage, scyllaConn *gocql.Session, keyspaceTTLMap map[int]string, logger *zap.Logger) *plot.Plot {

	plotService, err := plot.New(
		logger,
		timeseriesStats,
		scyllaConn,
		metadataStorage,
		conf.MaxTimeseries,
		conf.LogQueryTSthreshold,
		keyspaceTTLMap,
		conf.DefaultTTL,
		conf.DefaultPaginationSize,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createPlotService"),
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating plot service: %s", err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info("plot service was created", lf...)

	return plotService
}

// createUDPServer - creates the UDP server and starts it
func createUDPServer(conf *structs.SettingsUDP, collectorService *collector.Collector, stats *tsstats.StatsTS, logger *zap.Logger) *udp.UDPserver {

	udpServer := udp.New(logger, *conf, collectorService, stats)
	udpServer.Start()

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createUDPServer"),
	}

	logger.Info("udp server was created", lf...)

	return udpServer
}

// createRESTserver - creates the REST server and starts it
func createRESTserver(conf *structs.Settings, stats *snitch.Stats, plotService *plot.Plot, collectorService *collector.Collector, keyspaceManager *keyspace.Keyspace, keysetManager *keyset.KeySet, memcachedConn *memcached.Memcached, loggers *structs.Loggers) *rest.REST {

	restServer := rest.New(
		loggers,
		stats,
		plotService,
		keyspaceManager,
		memcachedConn,
		collectorService,
		conf.HTTPserver,
		conf.Probe.Threshold,
		keysetManager,
	)

	restServer.Start()

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createRESTserver"),
	}

	loggers.General.Info("rest server was created", lf...)

	return restServer
}

// createTelnetServer - creates a new telnet server
func createTelnetServer(conf *structs.SettingsTelnet, name string, telnetHandler telnetsrv.TelnetDataHandler, collectorService *collector.Collector, stats *tsstats.StatsTS, numActiveTelnetConnections *uint32, maxActiveTelnetConnections uint32, logger *zap.Logger) *telnetsrv.Server {

	telnetServer, err := telnetsrv.New(
		conf.Host,
		conf.Port,
		conf.OnErrorTimeout,
		conf.SendStatsTimeout,
		conf.MaxIdleConnectionTimeout,
		conf.MaxBufferSize,
		collectorService,
		stats,
		logger,
		numActiveTelnetConnections,
		maxActiveTelnetConnections,
		telnetHandler,
	)

	lf := []zapcore.Field{
		zap.String("package", "main"),
		zap.String("func", "createRESTserver"),
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("error creating telnet server '%s': %s", name, err.Error()), lf...)
		os.Exit(1)
	}

	err = telnetServer.Listen()
	if err != nil {
		logger.Fatal(fmt.Sprintf("error starting listening on telnet server '%s': %s", name, err.Error()), lf...)
		os.Exit(1)
	}

	logger.Info("telnet server was created: "+name, lf...)

	return telnetServer
}
