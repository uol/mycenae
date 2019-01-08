package cockroach

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/jinzhu/gorm"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	// postgres implementation
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// Connection - the connection manager
type Connection struct {
	dbConnection *gorm.DB
	logger       *zap.Logger
}

// NewConnUsingCerts - creates a new connection instance using certificate
func NewConnUsingCerts(host, database, user, certsPath string, logger *zap.Logger) (*Connection, error) {

	lf := []zapcore.Field{
		zap.String("struct", "Connection"),
		zap.String("func", "NewConnUsingCerts"),
	}

	connectionURL := fmt.Sprintf("postgresql://%s@%s/%s?ssl=true&sslmode=require&sslrootcert=%s/ca.crt&sslkey=%s/client.%s.key&sslcert=%s/client.%s.crt",
		user, host, database, certsPath, certsPath, user, certsPath, user)

	logger.Info("connecting to \""+connectionURL+"\"...", lf...)

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
func NewConnUsingPassword(host, database, user, password string, logger *zap.Logger) (*Connection, error) {

	lf := []zapcore.Field{
		zap.String("struct", "Connection"),
		zap.String("func", "NewConnUsingPassword"),
	}

	connectionURL := fmt.Sprintf("postgresql://%s:%s@%s/%s?ssl=true&sslmode=require",
		user, password, host, database)

	logger.Debug("connecting to \""+connectionURL+"\"...", lf...)

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
func NewConnUsingDocker(pod, network, database, user, password string, port int, logger *zap.Logger) (*Connection, error) {

	lf := []zapcore.Field{
		zap.String("struct", "Connection"),
		zap.String("func", "NewConnUsingPassword"),
	}

	output, err := exec.Command("docker", "inspect", "--format='{{ .NetworkSettings.Networks."+network+".IPAddress }}'", pod).Output()
	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}

	firstLine := strings.Replace(strings.Split(string(output), "\n")[0], "'", "", -1)

	matches, err := regexp.MatchString("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", firstLine)
	if err != nil {
		return nil, err
	}

	if !matches {
		err := fmt.Errorf("'%s' is not a valid IP", firstLine)
		logger.Error(err.Error(), lf...)
		return nil, err
	}

	connectionURL := fmt.Sprintf("%s:%d", firstLine, port)

	return NewConnUsingPassword(connectionURL, database, user, password, logger)
}

// GetConnection - returns the active connection
func (c *Connection) GetConnection() *gorm.DB {
	return c.dbConnection
}

// Close - closes the connection
func (c *Connection) Close() {

	lf := []zapcore.Field{
		zap.String("struct", "Connection"),
		zap.String("func", "Close"),
	}

	c.logger.Info("closing connection...", lf...)
	err := c.dbConnection.Close()
	if err != nil {
		c.logger.Error(err.Error(), lf...)
	} else {
		c.logger.Info("connection closed", lf...)
	}
}
