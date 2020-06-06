package timelinemanager_test

import (
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	gotesthttp "github.com/uol/gotest/http"
	gotesttelnet "github.com/uol/gotest/telnet"
	gotest "github.com/uol/gotest/utils"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
	"github.com/uol/timelinemanager"
)

const (
	testHost        string        = "localhost"
	channelSize     int           = 5
	bufferSize      int           = 256
	cycleDurationMS time.Duration = 200
	requestTimeoutS time.Duration = 5
)

type storageConfig struct {
	stype        timelinemanager.StorageType
	ttype        timelinemanager.TransportType
	httpServer   *gotesthttp.Server
	telnetServer *gotesttelnet.Server
	port         int
}

func (sc *storageConfig) Close() {

	if sc.httpServer != nil {
		sc.httpServer.Close()
	}

	if sc.telnetServer != nil {
		sc.telnetServer.Stop()
	}
}

func createTestConf(configs ...*storageConfig) *timelinemanager.Configuration {

	backends := []timelinemanager.BackendItem{}

	for _, conf := range configs {

		if conf.ttype == timelinemanager.HTTP {

			headers := http.Header{}
			headers.Add("Content-type", "text/plain; charset=utf-8")

			responses := []gotesthttp.ResponseData{
				{
					RequestData: gotesthttp.RequestData{
						URI:     "/post",
						Body:    "",
						Method:  "POST",
						Headers: headers,
					},
					Status: http.StatusOK,
				},
			}

			conf.port = gotest.GeneratePort()

			conf.httpServer = gotesthttp.NewServer(testHost, conf.port, channelSize, responses)

		} else if conf.ttype == timelinemanager.OpenTSDB {

			conf.telnetServer, conf.port = gotesttelnet.NewServer(
				&gotesttelnet.Configuration{
					Host:               testHost,
					MessageChannelSize: channelSize,
					ReadBufferSize:     bufferSize,
					ReadTimeout:        requestTimeoutS * time.Second,
				},
				true,
			)

		} else {
			panic("transport type is not defined")
		}

		backends = append(backends, timelinemanager.BackendItem{
			Backend: timeline.Backend{
				Host: testHost,
				Port: conf.port,
			},
			CycleDuration: funks.Duration{Duration: cycleDurationMS * time.Millisecond},
			Storage:       conf.stype,
			Type:          conf.ttype,
		})
	}

	c := &timelinemanager.Configuration{
		Backends:         backends,
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         12,
		DataTTL:          funks.Duration{Duration: time.Minute},
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			SerializerBufferSize: bufferSize,
			BatchSendInterval:    funks.Duration{Duration: cycleDurationMS * time.Millisecond},
			RequestTimeout:       funks.Duration{Duration: requestTimeoutS * time.Second},
			TransportBufferSize:  bufferSize,
			TimeBetweenBatches:   funks.Duration{Duration: 10 * time.Millisecond},
		},
		HTTPTransport: &timeline.HTTPTransportConfig{
			ExpectedResponseStatus: 200,
			Method:                 "POST",
			ServiceEndpoint:        "/post",
			TimestampProperty:      "timestamp",
			ValueProperty:          "value",
		},
		OpenTSDBTransport: &timeline.OpenTSDBTransportConfig{
			MaxReadTimeout:         funks.Duration{Duration: requestTimeoutS * time.Second},
			MaxReconnectionRetries: 3,
			ReadBufferSize:         bufferSize,
			ReconnectionTimeout:    funks.Duration{Duration: 100 * time.Millisecond},
		},
	}

	return c
}

func createTimelineManager(t *testing.T, configs ...*storageConfig) (*timelinemanager.Instance, bool) {

	tm, err := timelinemanager.New(createTestConf(configs...))
	if !assert.NoError(t, err, "expected no error creating the timeline manager") {
		return nil, false
	}

	if !assert.NotNil(t, tm, "expected a valid instance") {
		return nil, false
	}

	err = tm.Start()
	if !assert.NoError(t, err, "expected no error starting the timeline manager") {
		return nil, false
	}

	return tm, true
}

func closeAll(tm *timelinemanager.Instance, confs []*storageConfig) {

	<-time.After(requestTimeoutS * time.Second)

	if tm != nil {
		tm.Shutdown()
	}

	for _, conf := range confs {
		conf.Close()
	}
}

func testSendOpenTSDBMessage(
	t *testing.T,
	function string,
	tm *timelinemanager.Instance,
	stype timelinemanager.StorageType,
	op timeline.FlatOperation,

) (metric, tag1K, tag1V, tag2K, tag2V string, value int, testOk bool) {

	value = gotest.RandomInt(1, 100)
	metric = fmt.Sprintf("metric_%d", gotest.RandomInt(1, 100))

	tag1K = fmt.Sprintf("tag1_%d", gotest.RandomInt(1, 100))
	tag2K = fmt.Sprintf("tag2_%d", gotest.RandomInt(1, 100))

	tag1V = fmt.Sprintf("val1_%d", gotest.RandomInt(1, 100))
	tag2V = fmt.Sprintf("val2_%d", gotest.RandomInt(1, 100))

	err := tm.Send(
		function,
		stype,
		op,
		float64(value),
		metric,
		tag1K, tag1V,
		tag2K, tag2V,
	)

	testOk = assert.NoError(t, err, "expected no error")
	return
}

func testOpenTSDBMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig) bool {

	metric, tag1K, tag1V, tag2K, tag2V, value, ok := testSendOpenTSDBMessage(t, function, tm, stype, op)
	if !ok {
		return false
	}

	message := <-conf.telnetServer.MessageChannel()

	return assert.True(t,
		regexp.MustCompile(
			fmt.Sprintf(`put %s [0-9]{10} %d %s=%s %s=%s`,
				metric, value, tag1K, tag1V, tag2K, tag2V)).
			MatchString(message.Message),
		"expected same message",
	)
}

// TestOpenTSDB - creates a new manager telnet only
func TestOpenTSDB(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testOpenTSDBMessage(t, "TestOpenTSDB", tm, timelinemanager.Normal, timelinemanager.RawOpenTSDB, configs[0])
}

func testSendHTTPMessage(
	t *testing.T,
	function string,
	tm *timelinemanager.Instance,
	stype timelinemanager.StorageType,
	op timeline.FlatOperation,
	number bool,

) (metric string, value interface{}, testOk bool) {

	if number {
		value = gotest.RandomInt(1, 100)
	} else {
		value = fmt.Sprintf("text%d", gotest.RandomInt(1, 100))
	}

	metric = fmt.Sprintf("metric_%d", gotest.RandomInt(1, 100))

	tag1K := fmt.Sprintf("tag1K_%d", gotest.RandomInt(1, 100))
	tag2K := fmt.Sprintf("tag2K_%d", gotest.RandomInt(1, 100))

	tag1V := fmt.Sprintf("tag1V_%d", gotest.RandomInt(1, 100))
	tag2V := fmt.Sprintf("tag2V_%d", gotest.RandomInt(1, 100))

	var err error

	if number {

		err = tm.Send(
			function,
			stype,
			op,
			float64(value.(int)),
			metric,
			tag1K, tag1V,
			tag2K, tag2V,
		)

	} else {

		err = tm.SendText(
			function,
			stype,
			value.(string),
			metric,
			tag1K, tag1V,
			tag2K, tag2V,
		)
	}

	testOk = assert.NoError(t, err, "expected no error")
	return
}

func testHTTPMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig, number bool) bool {

	metric, value, ok := testSendHTTPMessage(t, function, tm, stype, op, number)
	if !ok {
		return false
	}

	message := <-conf.httpServer.RequestChannel()
	if !assert.NotNil(t, message, "expected a valid request message") {
		return false
	}

	if number {

		return assert.True(t,
			regexp.MustCompile(
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"value":%d?(\.[0]+)\}\]`,
					metric, value.(int))).
				MatchString(message.Body),
			"expected same message",
		)

	} else {

		return assert.True(t,
			regexp.MustCompile(
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"text":"%s"\}\]`,
					metric, value.(string))).
				MatchString(message.Body),
			"expected same message",
		)
	}
}

// TestHTTP - creates a new manager http only
func TestHTTP(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestHTTP", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testHTTPMessage(t, "TestHTTP", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
}

func testUnknownStorage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation) {

	err := tm.Send(function, stype, op, 1.0, "metric", "tag1", "val1", "tag2", "val2")
	assert.Error(t, err, "expected an error")

	assert.Equal(t, timelinemanager.ErrStorageNotFound, err, "expected timelinemanager.ErrStorageNotFound error type")
}

func testUnknownTransport(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, http bool, number bool) {

	var err error
	if !number {
		err = tm.SendText(function, stype, "test", "metric", "tag1", "val1", "tag2", "val2")
	} else {
		err = tm.Send(function, stype, op, 1, "metric", "tag1", "val1", "tag2", "val2")
	}

	assert.Error(t, err, "expected an error")

	assert.Equal(t, timelinemanager.ErrTransportNotSupported, err, "expected timelinemanager.ErrTransportNotSupported error type")
}

// TestStorageNotFound - creates a new manager and tests for a unknown storage
func TestStorageNotFound(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownStorage(t, "TestStorageNotFound", tm, timelinemanager.Archive, timelinemanager.RawHTTP)
}

// TestTransportNotSupported - creates a new manager and tests for a unknown transport
func TestTransportNotSupported(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownTransport(t, "TestTransportNotSupported", tm, timelinemanager.Normal, timelinemanager.RawHTTP, true, false)
}

// TestBothTransports - creates a new manager and tests http and opentsdb integration (no errors)
func TestBothTransports(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, "TestBothTransports", tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, configs[1])
}

// TestBothTransportsWithErrors - creates a new manager and tests http and opentsdb integration (some errors)
func TestBothTransportsWithErrors(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	funcName := "TestBothTransportsWithErrors"

	testUnknownStorage(t, funcName, tm, customStorage, timelinemanager.RawHTTP)
	testHTTPMessage(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawHTTP, configs[0], true)
	testUnknownTransport(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, true, true)
	testHTTPMessage(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, funcName, tm, timelinemanager.Normal, timelinemanager.RawOpenTSDB, configs[1])
	testUnknownTransport(t, funcName, tm, timelinemanager.Normal, timelinemanager.RawHTTP, true, true)
}

// TestSameBackendConfiguration - creates a new manager duplicating some backend
func TestSameBackendConfiguration(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, err := timelinemanager.New(createTestConf(configs...))
	if !assert.NoError(t, err, "expected no error creating the timeline manager") {
		return
	}

	if !assert.NotNil(t, tm, "expected a valid instance") {
		return
	}

	err = tm.Start()
	if !assert.Error(t, err, "expected an error starting the timeline manager") {
		return
	}

	assert.Equal(t, `backend named "archive" is registered more than one time`, err.Error(), "expected a specific error")
}

const customStorage timelinemanager.StorageType = "custom"

// TestTOMLConfiguration - tests loading the configuration as TOML
func TestTOMLConfiguration(t *testing.T) {

	conf := timelinemanager.Configuration{}

	_, err := toml.DecodeFile("./config.toml", &conf)
	if !assert.NoError(t, err, "no error expected reading the toml") {
		return
	}

	err = conf.Validate()
	if !assert.NoError(t, err, "no error expected validating the configuration") {
		return
	}

	// HashingAlgorithm hashing.Algorithm
	// HashSize         int
	// DataTTL          funks.Duration

	assert.Equal(t, gotest.MustParseDuration("2m"), conf.DataTTL.Duration, "DataTTL")
	assert.Equal(t, 6, conf.HashSize, "HashSize")
	assert.Equal(t, hashing.SHAKE128, conf.HashingAlgorithm, "HashingAlgorithm")

	// TransportBufferSize  int
	// BatchSendInterval    funks.Duration
	// RequestTimeout       funks.Duration
	// SerializerBufferSize int
	// DebugInput           bool
	// DebugOutput          bool
	// TimeBetweenBatches   funks.Duration

	assert.Equal(t, 1024, conf.TransportBufferSize, "TransportBufferSize")
	assert.Equal(t, gotest.MustParseDuration("30s"), conf.BatchSendInterval.Duration, "BatchSendInterval")
	assert.Equal(t, gotest.MustParseDuration("5s"), conf.RequestTimeout.Duration, "RequestTimeout")
	assert.Equal(t, 2048, conf.SerializerBufferSize, "SerializerBufferSize")
	assert.Equal(t, false, conf.DebugInput, "DebugInput")
	assert.Equal(t, true, conf.DebugOutput, "DebugOutput")
	assert.Equal(t, gotest.MustParseDuration("10ms"), conf.TimeBetweenBatches.Duration, "TimeBetweenBatches")

	// ReadBufferSize         int
	// MaxReadTimeout         funks.Duration
	// ReconnectionTimeout    funks.Duration
	// MaxReconnectionRetries int
	// DisconnectAfterWrites  bool

	assert.True(t, reflect.DeepEqual(conf.DefaultTransportConfiguration, conf.OpenTSDBTransport.DefaultTransportConfiguration), "expected same object")
	assert.Equal(t, gotest.MustParseDuration("100ms"), conf.OpenTSDBTransport.MaxReadTimeout.Duration, "MaxReadTimeout")
	assert.Equal(t, 5, conf.OpenTSDBTransport.MaxReconnectionRetries, "MaxReconnectionRetries")
	assert.Equal(t, 64, conf.OpenTSDBTransport.ReadBufferSize, "ReadBufferSize")
	assert.Equal(t, gotest.MustParseDuration("3s"), conf.OpenTSDBTransport.ReconnectionTimeout.Duration, "ReconnectionTimeout")
	assert.Equal(t, true, conf.OpenTSDBTransport.DisconnectAfterWrites, "DisconnectAfterWrites")
	assert.Equal(t, true, conf.OpenTSDBTransport.PrintStackOnError, "PrintStackOnError")

	// ServiceEndpoint        string
	// Method                 string
	// ExpectedResponseStatus int
	// TimestampProperty      string
	// ValueProperty          string

	assert.True(t, reflect.DeepEqual(conf.DefaultTransportConfiguration, conf.HTTPTransport.DefaultTransportConfiguration), "expected same object")
	assert.Equal(t, "/api/put", conf.HTTPTransport.ServiceEndpoint, "ServiceEndpoint")
	assert.Equal(t, "POST", conf.HTTPTransport.Method, "Method")
	assert.Equal(t, 204, conf.HTTPTransport.ExpectedResponseStatus, "ExpectedResponseStatus")
	assert.Equal(t, "timestamp", conf.HTTPTransport.TimestampProperty, "TimestampProperty")
	assert.Equal(t, "value", conf.HTTPTransport.ValueProperty, "ValueProperty")
	assert.Equal(t, true, conf.HTTPTransport.PrintStackOnError, "PrintStackOnError")

	var host1Found, host2Found, host3Found bool

	for _, backend := range conf.Backends {

		var expected timelinemanager.BackendItem

		if backend.Host == "host1" {

			host1Found = true
			expected.Host = backend.Host
			expected.AddHostTag = true
			expected.CycleDuration = funks.Duration{Duration: gotest.MustParseDuration("15s")}
			expected.Port = 8123
			expected.Storage = timelinemanager.Normal
			expected.Type = timelinemanager.OpenTSDB
			expected.CommonTags = map[string]string{
				"tag1": "val1",
				"tag2": "val2",
				"tag3": "val3",
			}

		} else if backend.Host == "host2" {

			host2Found = true
			expected.Host = backend.Host
			expected.AddHostTag = true
			expected.CycleDuration = funks.Duration{Duration: gotest.MustParseDuration("25s")}
			expected.Port = 8124
			expected.Storage = timelinemanager.Archive
			expected.Type = timelinemanager.OpenTSDB
			expected.CommonTags = map[string]string{
				"tag4": "val4",
				"tag5": "val5",
				"tag6": "val6",
			}

		} else if backend.Host == "host3" {

			host3Found = true
			expected.Host = backend.Host
			expected.AddHostTag = false
			expected.CycleDuration = funks.Duration{Duration: gotest.MustParseDuration("35s")}
			expected.Port = 8125
			expected.Storage = customStorage
			expected.Type = timelinemanager.HTTP
			expected.CommonTags = map[string]string{
				"tag7": "val7",
				"tag8": "val8",
				"tag9": "val9",
			}

		} else {

			assert.Fail(t, "unexpected host", "unexpected host found: %s", backend.Host)
			continue
		}

		assert.Equal(t, expected.AddHostTag, backend.AddHostTag, expected.Host+"->AddHostTag")
		assert.Equal(t, expected.CycleDuration, backend.CycleDuration, expected.Host+"->CycleDuration")
		assert.Equal(t, expected.Port, backend.Port, expected.Host+"->Port")
		assert.Equal(t, expected.Storage, backend.Storage, expected.Host+"->Storage")
		assert.Equal(t, expected.Type, backend.Type, expected.Host+"->Type")
		assert.Len(t, backend.CommonTags, 3, expected.Host+"->CommonTags(len)")
		assert.True(t, reflect.DeepEqual(expected.CommonTags, backend.CommonTags), expected.Host+"->CommonTags(content)")
	}

	assert.True(t, host1Found, "expected host1 backend configuration")
	assert.True(t, host2Found, "expected host2 backend configuration")
	assert.True(t, host3Found, "expected host3 backend configuration")
}
