package tools

import (
	"log"
	"time"

	"github.com/gocql/gocql"
)

type cassTool struct {
	Timeseries *cassTs
}

func (cass *cassTool) init(set CassandraSettings) {
	cluster := gocql.NewCluster(set.Nodes...)

	cluster.ProtoVersion = set.ProtoVersion
	cluster.Keyspace = set.Keyspace

	switch set.Consistency {
	case "one":
		cluster.Consistency = gocql.One
	case "quorum":
		cluster.Consistency = gocql.Quorum
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: set.Username,
		Password: set.Password,
	}

	cluster.NumConns = set.Connections
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: set.Retry}

	if set.Timeout != "" {
		t, err := time.ParseDuration(set.Timeout)

		if err != nil {
			return
		}

		cluster.Timeout = t
	}

	cc, err := cluster.CreateSession()
	if err != nil {
		log.Println(err)
		panic(err.Error())
	}

	if set.PageSize != 0 {
		cc.SetPageSize(set.PageSize)
	}

	ts := new(cassTs)
	ts.init(cc)
	cass.Timeseries = ts

	return
}
