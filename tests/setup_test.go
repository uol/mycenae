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

func createKeysetName() string {
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

	mycenaeTools.InitHTTP("http://mycenae", "8082", 3*time.Minute)

	mycenaeTools.InitUDP("mycenae", "4243")

	mycenaeTools.InitMycenae(tools.RestAPISettings{
		Node:    "http://mycenae",
		Port:    "8082",
		Timeout: 5 * time.Minute,
	})

	mycenaeTools.InitSolr(tools.RestAPISettings{
		Node:    "http://solr",
		Port:    "8983",
		Timeout: time.Minute,
	})

	flag.Parse()

	if !skipSetup {

		var wg sync.WaitGroup
		// ksMycenae = mycenaeTools.Mycenae.CreateKeyset(createKeysetName())
		// ksMycenaeMeta = mycenaeTools.Mycenae.CreateKeyset(createKeysetName())
		// ksMycenaeTsdb = mycenaeTools.Mycenae.CreateKeyset(createKeysetName())
		ksTTLKeyspace = mycenaeTools.Mycenae.CreateKeyset(createKeysetName())

		wg.Add(1)

		// go func() { sendPointsExpandExp(ksMycenae); wg.Done() }()
		// go func() { sendPointsMetadata(ksMycenaeMeta); wg.Done() }()
		// go func() { sendPointsParseExp(ksMycenae); wg.Done() }()
		// go func() { sendPointsPointsGrafana(ksMycenae); wg.Done() }()
		// ** go func() { sendPointsPointsGrafanaMem(ksMycenae); wg.Done() }()
		// go func() { sendPointsTsdbAggAndSugAndLookup(ksMycenaeTsdb); wg.Done() }()
		// go func() { sendPointsV2(ksMycenae); wg.Done() }()
		// go func() { sendPointsV2Text(ksMycenae); wg.Done() }()
		go func() { sendPointsToTTLKeyspace(ksTTLKeyspace); wg.Done() }()

		wg.Wait()

		// time.Sleep(time.Second * 30)
	}

	os.Exit(m.Run())
}
