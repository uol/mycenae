package cockroach

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/jinzhu/gorm"
	"github.com/uol/logh"
)

//
// A helper to create connection to the Cockroach DB
// author: rnojiri
//

// Connection - the connection manager
type Connection struct {
	dbConnection *gorm.DB
	logger       *logh.ContextualLogger
}

// NewConnUsingCerts - creates a new connection instance using certificate
func NewConnUsingCerts(host, database, user, certsPath string) (*Connection, error) {

	logger := logh.CreateContextualLogger("pkg", "cockroach")

	connectionURL := fmt.Sprintf("postgresql://%s@%s/%s?ssl=true&sslmode=require&sslrootcert=%s/ca.crt&sslkey=%s/client.%s.key&sslcert=%s/client.%s.crt",
		user, host, database, certsPath, certsPath, user, certsPath, user)

	if logh.InfoEnabled {
		logger.Info().Str("func", "NewConnUsingCerts").Msgf("connecting to \"%s\"...", connectionURL)
	}

	db, err := gorm.Open("postgres", connectionURL)
	if err != nil {
		return nil, err
	}

	return &Connection{
		dbConnection: db,
		logger:       logger,
	}, nil
}

// NewConnUsingPassword - creates a new connection instance using password
func NewConnUsingPassword(host, database, user, password string) (*Connection, error) {

	logger := logh.CreateContextualLogger("pkg", "cockroach")

	connectionURL := fmt.Sprintf("postgresql://%s:%s@%s/%s?ssl=true&sslmode=require",
		user, password, host, database)

	if logh.InfoEnabled {
		logger.Info().Str("func", "NewConnUsingPassword").Msgf("connecting to \"%s\"...", connectionURL)
	}

	db, err := gorm.Open("postgres", connectionURL)
	if err != nil {
		return nil, err
	}

	return &Connection{
		dbConnection: db,
		logger:       logger,
	}, nil
}

// NewConnUsingDocker - creates a new connection instance to a local docker pod
func NewConnUsingDocker(pod, network, database, user, password string, port int) (*Connection, error) {

	logger := logh.CreateContextualLogger("pkg", "cockroach")

	output, err := exec.Command("docker", "inspect", "--format='{{ .NetworkSettings.Networks."+network+".IPAddress }}'", pod).Output()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Str("func", "NewConnUsingDocker").Msgf("connecting to \"%s\"...", pod)
		}
		panic(err)
	}

	firstLine := strings.Replace(strings.Split(string(output), "\n")[0], "'", "", -1)

	matches, err := regexp.MatchString("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", firstLine)
	if err != nil {
		return nil, err
	}

	if !matches {
		err := fmt.Errorf("'%s' is not a valid IP", firstLine)
		if logh.ErrorEnabled {
			logger.Error().Str("func", "NewConnUsingDocker").Err(err).Msgf("error connecting to \"%s\"...", pod)
		}
		return nil, err
	}

	connectionURL := fmt.Sprintf("%s:%d", firstLine, port)

	return NewConnUsingPassword(connectionURL, database, user, password)
}

// GetConnection - returns the active connection
func (c *Connection) GetConnection() *gorm.DB {
	return c.dbConnection
}

// Close - closes the connection
func (c *Connection) Close() {

	if logh.InfoEnabled {
		c.logger.Info().Str("func", "Close").Msg("closing connection...")
	}

	err := c.dbConnection.Close()
	if err != nil {
		if logh.ErrorEnabled {
			c.logger.Error().Str("func", "Close").Err(err).Send()
		}
	} else {
		if logh.InfoEnabled {
			c.logger.Info().Str("func", "Close").Msg("connection closed")
		}
	}
}
