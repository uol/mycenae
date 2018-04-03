package rubber

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"go.uber.org/zap"
)

type esResponse struct {
	status  int
	content []byte
	err     error
}

type esRequest struct {
	index  string
	method string
	path   string
	body   io.Reader

	answ    chan esResponse
	retries uint64
}

type consumer struct {
	server string
	index  string
	client *http.Client
	logger *zap.Logger

	input    chan *esRequest
	shutdown chan bool

	errorTimeout time.Duration
	maxRetries   uint64
}

func (c *consumer) loop() error {
	for {
		select {
		case request := <-c.input:
			c.logger.Debug(
				"Request executed",
				zap.String("function", "loop"),
				zap.String("structure", "consumer"),
				zap.String("package", "rubber"),
				zap.String("index", c.index),
				zap.String("rindex", request.index),
			)
			status, content, err := c.Request(c.server, request.method,
				path.Join("/", request.index, request.path), request.body)
			if err != nil && request.retries < c.maxRetries {
				c.logger.Warn("Retry request",
					zap.String("function", "loop"),
					zap.String("structure", "consumer"),
					zap.String("package", "rubber"),
				)
				request.retries++
				c.input <- request
				time.Sleep(c.errorTimeout)
				continue
			}
			request.answ <- esResponse{
				status:  status,
				content: content,
				err:     err,
			}
		case <-c.shutdown:
			c.logger.Debug(
				"Shutdown consumer",
				zap.String("function", "loop"),
				zap.String("structure", "consumer"),
				zap.String("package", "rubber"),
			)
			c.shutdown <- true
			return nil
		}
	}
}

func (c *consumer) close() error {
	c.shutdown <- true
	<-c.shutdown
	close(c.shutdown)
	return nil
}

func (c *consumer) Request(server, method, urlPath string, body io.Reader) (int, []byte, error) {

	url := fmt.Sprintf("http://%s%s", server, path.Join("/", urlPath))

	request, err := http.NewRequest(method, url, body)
	if err != nil {
		c.logger.Error(
			err.Error(),
			zap.String("package", "rubber"),
			zap.String("structure", "consumer"),
			zap.String("function", "Request"),
			zap.Error(err),
		)
		return 0, nil, err
	}
	// start := time.Now()
	resp, err := c.client.Do(request)
	// end := time.Now()

	if err != nil {
		c.logger.Error(
			err.Error(),
			zap.String("package", "rubber"),
			zap.String("structure", "consumer"),
			zap.String("function", "Request"),
			zap.Error(err),
		)
		return 0, nil, err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)

	c.logger.Debug(
		"Request without error",
		zap.String("package", "rubber"),
		zap.String("structure", "consumer"),
		zap.String("function", "Request"),
	)

	return resp.StatusCode, content, err
}
