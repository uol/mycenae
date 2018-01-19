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

var setup = flag.Bool("setup", true, "flag used to skip setup when set to false")
var mycenaeTools tools.Tool
var ksMycenae, ksMycenaeMeta, ksMycenaeTsdb, ksTTLKeyspace string

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
		Timeout:        "60s",
		ProtoVersion:   4,
	})

	mycenaeTools.InitHTTP("http://mycenae", "8787", time.Minute)

	mycenaeTools.InitUDP("mycenae", "4243")

	mycenaeTools.InitMycenae(tools.MycenaeSettings{
		Node:    "http://mycenae",
		Port:    "8787",
		Timeout: time.Minute,
	})

	mycenaeTools.InitEs(tools.ElasticsearchSettings{
		Node:    "http://elasticsearch",
		Port:    "9200",
		Timeout: 20 * time.Second,
	})

	flag.Parse()

	ksMycenae = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())

	if *setup {

		var wg sync.WaitGroup

		ksMycenaeMeta = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())
		ksMycenaeTsdb = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())
		ksTTLKeyspace = mycenaeTools.Mycenae.CreateKeySet(createKeySetName())

		wg.Add(8)

		go func() { sendPointsExpandExp(ksMycenae); wg.Done() }()
		go func() { sendPointsMetadata(ksMycenaeMeta); wg.Done() }()
		go func() { sendPointsParseExp(ksMycenae); wg.Done() }()
		go func() { sendPointsPointsGrafana(ksMycenae); wg.Done() }()
		//go func() { sendPointsPointsGrafanaMem(ksMycenae); wg.Done() }()
		go func() { sendPointsTsdbAggAndSugAndLookup(ksMycenaeTsdb); wg.Done() }()
		go func() { sendPointsV2(ksMycenae); wg.Done() }()
		go func() { sendPointsV2Text(ksMycenae); wg.Done() }()
		go func() { sendPointsToTTLKeyspace(ksTTLKeyspace); wg.Done() }()

		wg.Wait()

		time.Sleep(time.Second * 20)
	}

	os.Exit(m.Run())
}
