package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/loader"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/saw"
	"github.com/uol/gobol/snitch"

	"github.com/uol/mycenae/lib/cache"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/memcached"
	"github.com/uol/mycenae/lib/metadata"
	"github.com/uol/mycenae/lib/persistence"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/rest"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/udp"
)

func main() {
	fmt.Println("Starting Mycenae")
	//Parse of command line arguments.
	var confPath string

	flag.StringVar(&confPath, "config", "config.toml", "path to configuration file")
	flag.Parse()

	//Load conf file.
	settings := new(structs.Settings)

	err := loader.ConfToml(confPath, &settings)
	if err != nil {
		log.Fatalln("ERROR - Loading Config file: ", err)
	} else {
		fmt.Println("Config file loaded.")
	}

	tsLogger := new(structs.TsLog)
	tsLogger.General, err = saw.New(settings.Logs.General)
	if err != nil {
		log.Fatalln("ERROR - Starting logger: ", err)
	}

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6666", nil))
	}()

	tsLogger.Stats, err = saw.New(settings.Logs.Stats)
	if err != nil {
		log.Fatalln("ERROR - Starting logger: ", err)
	}

	sts, err := snitch.New(tsLogger.Stats, settings.Stats)
	if err != nil {
		log.Fatalln("ERROR - Starting stats: ", err)
	}

	tssts, err := tsstats.New(tsLogger.General, sts, settings.Stats.Interval)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	cass, err := cassandra.New(settings.Cassandra)
	if err != nil {
		tsLogger.General.Error("ERROR - Connecting to cassandra: ", err)
		os.Exit(1)
	}
	defer cass.Close()

	// --- Including metadata and persistence ---
	meta, err := metadata.Create(
		settings.ElasticSearch.Cluster,
		tsLogger.General,
		tssts,
	)
	if err != nil {
		tsLogger.General.Fatalf("Error creating metadata backend")
	}

	storage, err := persistence.NewStorage(
		settings.Cassandra.Keyspace,
		settings.Cassandra.Username,
		tsLogger.General,
		cass,
		meta,
		tssts,
	)
	if err != nil {
		tsLogger.General.Fatalf("Error creating persistence backend")
	}

	_ = storage
	// --- End of metadata and persistence ---

	es, err := rubber.New(tsLogger.General, settings.ElasticSearch.Cluster)
	if err != nil {
		tsLogger.General.Error("ERROR - Connecting to elasticsearch: ", err)
		os.Exit(1)
	}

	ks := keyspace.New(
		tssts,
		storage,
		settings.Cassandra.Username,
		settings.Cassandra.Keyspace,
		settings.TTL.Max,
	)

	mc, err := memcached.New(tssts, &settings.Memcached)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	kc := cache.NewKeyspaceCache(mc, ks)

	coll, err := collector.New(tsLogger, tssts, cass, es, kc, settings)
	if err != nil {
		log.Println(err)
		return
	}

	uV2server := udp.New(tsLogger.General, settings.UDPserverV2, coll)
	uV2server.Start()

	collectorV1 := collector.UDPv1{}
	uV1server := udp.New(tsLogger.General, settings.UDPserver, collectorV1)
	uV1server.Start()

	p, err := plot.New(
		tsLogger.General,
		tssts,
		cass,
		es,
		kc,
		settings.ElasticSearch.Index,
		settings.MaxTimeseries,
		settings.MaxConcurrentTimeseries,
		settings.MaxConcurrentReads,
		settings.LogQueryTSthreshold,
	)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	tsRest := rest.New(
		tsLogger,
		sts,
		p,
		ks,
		mc,
		coll,
		settings.HTTPserver,
		settings.Probe.Threshold,
	)
	tsRest.Start()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	fmt.Println("Mycenae started successfully")
	for {
		sig := <-signalChannel
		switch sig {
		case os.Interrupt, syscall.SIGTERM:
			stop(tsLogger, tsRest, coll)
			return
		case syscall.SIGHUP:
			//THIS IS A HACK DO NOT EXTEND IT. THE FEATURE IS NICE BUT NEEDS TO BE DONE CORRECTLY!!!!!
			settings := new(structs.Settings)
			var err error

			if strings.HasSuffix(confPath, ".json") {
				err = loader.ConfJson(confPath, &settings)
			} else if strings.HasSuffix(confPath, ".toml") {
				err = loader.ConfToml(confPath, &settings)
			}
			if err != nil {
				tsLogger.General.Error("ERROR - Loading Config file: ", err)
				continue
			} else {
				tsLogger.General.Info("Config file loaded.")
			}
		}
	}
}

func stop(logger *structs.TsLog, rest *rest.REST, collector *collector.Collector) {

	fmt.Println("Stopping REST")
	logger.General.Info("Stopping REST")
	rest.Stop()
	fmt.Println("REST stopped")

	fmt.Println("Stopping UDPv2")
	logger.General.Info("Stopping UDPv2")
	collector.Stop()
	fmt.Println("UDPv2 stopped")

}
