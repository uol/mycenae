package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/uol/mycenae/tests/tools"
)

var skipSetup = false
var mycenaeTools tools.Tool
var ksMycenae, ksMycenaeMeta, ksMycenaeTsdb, ksTTLKeyspace string

const datacenter = "dc_gt_a1"

func createKeySetName() string {
	return fmt.Sprintf("ts_%d", time.Now().Nanosecond())
}

func TestMain(m *testing.M) {

	mycenaeTools.InitCass(tools.CassandraSettings{
		Keyspace:    "mycenae",
		Consistency: "quorum",
		Nodes:       []string{"scylla1", "scylla2", "scylla3"},

		Username:    "cassandra",
		Password:    "cassandra",
		Connections: 3,
		Retry:       5,
		PageSize:    1000,

		DiscoverHosts:  true,
		DiscoverySleep: 10,
		Timeout:        "1m",
		ProtoVersion:   4,
	})

	mycenaeTools.InitHTTP("http://mycenae", "8080", 3*time.Minute)

	mycenaeTools.InitUDP("mycenae", "4243")

	mycenaeTools.InitMycenae(tools.RestAPISettings{
		Node:    "http://mycenae",
		Port:    "8080",
		Timeout: 5 * time.Minute,
	})

	mycenaeTools.InitSolr(tools.RestAPISettings{
		Node:    "http://solr",
		Port:    "8983",
		Timeout: time.Minute,
	})

	flag.Parse()

	ksMycenae = "ts_312486240"
	ksMycenaeMeta = "ts_439299136"
	ksMycenaeTsdb = "ts_75996059"
	ksTTLKeyspace = "ts_647688581"

	if !skipSetup {

		var wg sync.WaitGroup

		ksMycenae = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())
		ksMycenaeMeta = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())
		ksMycenaeTsdb = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())
		ksTTLKeyspace = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())

		wg.Add(8)

		go func() { sendPointsExpandExp(ksMycenae); wg.Done() }()
		go func() { sendPointsMetadata(ksMycenaeMeta); wg.Done() }()
		go func() { sendPointsParseExp(ksMycenae); wg.Done() }()
		go func() { sendPointsPointsGrafana(ksMycenae); wg.Done() }()
		// go func() { sendPointsPointsGrafanaMem(ksMycenae); wg.Done() }()
		go func() { sendPointsTsdbAggAndSugAndLookup(ksMycenaeTsdb); wg.Done() }()
		go func() { sendPointsV2(ksMycenae); wg.Done() }()
		go func() { sendPointsV2Text(ksMycenae); wg.Done() }()
		go func() { sendPointsToTTLKeyspace(ksTTLKeyspace); wg.Done() }()

		wg.Wait()

		time.Sleep(time.Second * 20)
	}

	os.Exit(m.Run())
}
