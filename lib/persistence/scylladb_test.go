package persistence

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/snitch"
	"github.com/uol/mycenae/lib/tsstats"
)

func createBasicTables(session *gocql.Session) error {
	for _, query := range basicEnvironmentQueries {
		if err := session.Query(query).Exec(); err != nil {
			fmt.Fprintf(os.Stderr, "Query: %s\n", query)
			return err
		}
	}
	return nil
}

func TestScylladbBackend(t *testing.T) {
	const (
		username = ""
		password = ""
	)
	scyllaAddress := os.Getenv("SCYLLA_IP")
	if len(scyllaAddress) <= 0 {
		t.SkipNow()
	}

	logger := logrus.New()
	if !assert.NotNil(t, logger) {
		return
	}

	gstats, err := snitch.New(logger, snitch.Settings{
		Address:  "localhost",
		Interval: "@every 1m",
		KSID:     "macstest",
		Port:     "4243",
		Protocol: "udp",
		Runtime:  true,
		Tags: map[string]string{
			"service": "mycenae-dev-test",
		},
	})
	if !assert.NotNil(t, gstats) || !assert.NoError(t, err) {
		return
	}

	stats, err := tsstats.New(logger, gstats, "* * * * *")
	if !assert.NotNil(t, stats) || !assert.NoError(t, err) {
		return
	}

	cluster := gocql.NewCluster(scyllaAddress)
	cluster.ProtoVersion = 3
	cluster.Timeout = 20 * time.Second
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	session, err := cluster.CreateSession()
	if !assert.NotNil(t, stats) || !assert.NoError(t, err) {
		return
	}
	defer session.Close()

	if !assert.NoError(t, createBasicTables(session)) {
		return
	}

	backend, err := newScyllaPersistence(
		scyllaMainKeyspace, username,
		session, logger, stats,
	)
	if assert.NotNil(t, backend) && assert.NoError(t, err) {
		genericPersistenceBackendTest(t, backend, logger)
	}
}
