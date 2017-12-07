package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var errKsName = "Wrong Format: Field \"keyspaceName\" is not well formed. NO information will be saved"
var errKsRF = "Replication factor can not be less than or equal to 0 or greater than 3"
var errKsDC = "Cannot create because datacenter \"dc_error\" not exists"
var errKsDCNil = "Datacenter can not be empty or nil"
var errKsContact = "Contact field should be a valid email address"
var errKsTTL = "TTL can not be less or equal to zero"
var errKsTTLMax = "Max TTL allowed is 90"

func getKeyspace() tools.Keyspace {

	data := tools.Keyspace{
		Name:              getRandName(),
		Datacenter:        "docker-dc",
		ReplicationFactor: 1,
		Contact:           fmt.Sprintf("test-%d@domain.com", time.Now().Unix()),
		TTL:               90,
	}

	return data
}

func getRandName() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%d", rand.Int())
}

func testKeyspaceCreation(data *tools.Keyspace, t *testing.T) {

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, resp, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}
	assert.Equal(t, 201, code)

	var ksr tools.KeyspaceResp
	err = json.Unmarshal(resp, &ksr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	time.Sleep(time.Second * 10)

	data.ID = ksr.KSID
	assert.Contains(t, data.ID, "ts_", "Keyspace ID was poorly formed")
	assert.Equal(t, 1, mycenaeTools.Cassandra.Timeseries.CountTsKeyspaceByKsid(data.ID))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.ID), fmt.Sprintf("Keyspace %v was not created", data.ID))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.ExistsInformation(data.ID, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")
}

func testKeyspaceCreationFail(data []byte, keyName string, response tools.Error, test string, t *testing.T) {

	path := fmt.Sprintf("keyspaces/%s", keyName)
	code, resp, err := mycenaeTools.HTTP.POST(path, data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, test)
	assert.Equal(t, response, respErr, test)
	//assert.Equal(t, 0, mycenaeTools.Cassandra.Timeseries.CountTsKeyspaceByName(keyName), test)
}

func testKeyspaceEdition(id string, data tools.KeyspaceEdit, t *testing.T) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", id)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)
	assert.True(t, ksBefore != ksAfter)
	assert.Equal(t, data.Name, ksAfter.Name)
	assert.Equal(t, data.Contact, ksAfter.Contact)
}

func testKeyspaceEditionConcurrently(id string, data1 tools.KeyspaceEdit, data2 tools.KeyspaceEdit, t *testing.T, wg *sync.WaitGroup) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	body, err := json.Marshal(data1)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", id)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)
	assert.True(t, ksBefore != ksAfter)
	assert.True(t, data1.Name == ksAfter.Name || data2.Name == ksAfter.Name)
	assert.True(t, data1.Contact == ksAfter.Contact || data2.Contact == ksAfter.Contact)

	wg.Done()
}

func testKeyspaceEditionFail(id string, data []byte, status int, response tools.Error, test string, t *testing.T) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	path := fmt.Sprintf("keyspaces/%s", id)
	code, resp, err := mycenaeTools.HTTP.PUT(path, data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	assert.Equal(t, status, code, test)
	assert.Equal(t, response, respErr, test)
	assert.True(t, ksBefore == ksAfter, test)
}

func checkKeyspacePropertiesAndIndex(data tools.Keyspace, t *testing.T) {

	var tables = []string{"ts_number_stamp", "ts_text_stamp"}
	var elasticSearchIndexMeta = "\"meta\":{\"properties\":{\"tagsNested\":{\"type\":\"nested\",\"properties\":{\"tagKey\":{\"type\":\"string\"},\"tagValue\":{\"type\":\"string\"}}}}}"
	var elasticSearchIndexMetaText = "\"metatext\":{\"properties\":{\"tagsNested\":{\"type\":\"nested\",\"properties\":{\"tagKey\":{\"type\":\"string\"},\"tagValue\":{\"type\":\"string\"}}}}}"
	var caching = map[string]string{
		"keys":               "ALL",
		"rows_per_partition": "ALL",
	}
	var compaction = map[string]string{
		"max_threshold":          "64",
		"min_threshold":          "8",
		"class":                  "TimeWindowCompactionStrategy",
		"compaction_window_size": "7",
		"compaction_window_unit": "DAYS",
	}
	var compression = map[string]string{
		"crc_check_chance": "0.500000",
		"sstable_compression": "org.apache.cassandra.io.compress.LZ4Compressor",
	}

	var replication = map[string]string{
		"class":       "org.apache.cassandra.locator.NetworkTopologyStrategy",
		"docker-dc": fmt.Sprintf("%v", data.ReplicationFactor),
	}

	esIndexResponse := mycenaeTools.ElasticSearch.Keyspace.GetIndex(data.ID)
	tableProperties := mycenaeTools.Cassandra.Timeseries.TableProperties(data.ID, "ts_number_stamp")
	ksProperties := mycenaeTools.Cassandra.Timeseries.KeyspaceProperties(data.ID)

	for _, table := range tables {
		tableProperties = mycenaeTools.Cassandra.Timeseries.TableProperties(data.ID, table)
		assert.Exactly(t, 0.01, tableProperties.Bloom_filter_fp_chance)
		assert.Exactly(t, caching, tableProperties.Caching)
		assert.Exactly(t, "", tableProperties.Comment)
		assert.Exactly(t, compaction, tableProperties.Compaction)
		assert.Exactly(t, compression, tableProperties.Compression)
		assert.Exactly(t, 0.0, tableProperties.Dclocal_read_repair_chance)
		assert.Exactly(t, data.TTL*86400, tableProperties.Default_time_to_live)
		assert.Exactly(t, 0, tableProperties.Gc_grace_seconds)
		assert.Exactly(t, 2048, tableProperties.Max_index_interval)
		assert.Exactly(t, 0, tableProperties.Memtable_flush_period_in_ms)
		assert.Exactly(t, 128, tableProperties.Min_index_interval)
		assert.Exactly(t, 0.0, tableProperties.Read_repair_chance)
		assert.Exactly(t, "99.0PERCENTILE", tableProperties.Speculative_retry)
	}

	assert.Exactly(t, replication, ksProperties.Replication)
	assert.Contains(t, data.ID, "ts_", "We received a weird keyspace name for request")
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.ID), "Keyspace was not created")

	keyspaceCassandraTables := mycenaeTools.Cassandra.Timeseries.KeyspaceTables(data.ID)
	sort.Strings(keyspaceCassandraTables)
	sort.Strings(tables)
	assert.Equal(t, tables, keyspaceCassandraTables)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMeta)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMetaText)
}

// CREATE

func TestKeyspaceCreateSuccessRF1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 1
	testKeyspaceCreation(&data, t)
	checkKeyspacePropertiesAndIndex(data, t)
}

func TestKeyspaceCreateSuccessRF2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 2
	testKeyspaceCreation(&data, t)
	checkKeyspacePropertiesAndIndex(data, t)
}

func TestKeyspaceCreateSuccessRF3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 3
	testKeyspaceCreation(&data, t)
	checkKeyspacePropertiesAndIndex(data, t)
}

func TestKeyspaceCreateFailDCError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		rf      = 1
		ttl     = 90
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"DCNil":      {Datacenter: "", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
		"EmptyDC":    {ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
		"DCNotExist": {Datacenter: "dc_error", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
	}

	errNil := tools.Error{Error: errKsDCNil, Message: errKsDCNil}
	errNotExist := tools.Error{Error: errKsDC, Message: errKsDC}

	for test, ks := range cases {

		if test == "DCNotExist" {

			testKeyspaceCreationFail(ks.Marshal(), ks.Name, errNotExist, test, t)
		} else {

			testKeyspaceCreationFail(ks.Marshal(), ks.Name, errNil, test, t)
		}
	}
}

func TestKeyspaceCreateFailNameError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		rf      = 1
		ttl     = 90
		dc      = "docker-dc"
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"BadName*": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "test_*123"},
		"_BadName": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "_test123"},
	}

	err := tools.Error{Error: errKsName, Message: errKsName}

	for test, ks := range cases {
		testKeyspaceCreationFail(ks.Marshal(), ks.Name, err, test, t)
	}
}

func TestKeyspaceCreateFailRFError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		ttl     = 90
		dc      = "docker-dc"
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"RF0":        {Datacenter: dc, ReplicationFactor: 0, TTL: ttl, Contact: contact, Name: getRandName()},
		"RF4":        {Datacenter: dc, ReplicationFactor: 4, TTL: ttl, Contact: contact, Name: getRandName()},
		"RFNil":      {Datacenter: dc, TTL: ttl, Contact: contact, Name: getRandName()},
		"NegativeRF": {Datacenter: dc, ReplicationFactor: -1, TTL: ttl, Contact: contact, Name: getRandName()},
	}

	err := tools.Error{Error: errKsRF, Message: errKsRF}

	for test, ks := range cases {
		testKeyspaceCreationFail(ks.Marshal(), ks.Name, err, test, t)
	}
}

func TestKeyspaceCreateFailTTLError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		rf      = 1
		dc      = "docker-dc"
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"TTL0":        {Datacenter: dc, ReplicationFactor: rf, TTL: 0, Contact: contact, Name: getRandName()},
		"TTLNil":      {Datacenter: dc, ReplicationFactor: rf, Contact: contact, Name: getRandName()},
		"TTLAboveMax": {Datacenter: dc, ReplicationFactor: rf, TTL: 91, Contact: contact, Name: getRandName()},
		"NegativeTTL": {Datacenter: dc, ReplicationFactor: rf, TTL: -10, Contact: contact, Name: getRandName()},
	}

	errTTL := tools.Error{Error: errKsTTL, Message: errKsTTL}
	errTTLMax := tools.Error{Error: errKsTTLMax, Message: errKsTTLMax}

	for test, ks := range cases {

		if test == "TTLAboveMax" {
			testKeyspaceCreationFail(ks.Marshal(), ks.Name, errTTLMax, test, t)
		} else {
			testKeyspaceCreationFail(ks.Marshal(), ks.Name, errTTL, test, t)
		}
	}
}

func TestKeyspaceCreateFailContactError(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		rf  = 1
		ttl = 90
		dc  = "docker-dc"
	)

	cases := map[string]tools.Keyspace{
		"ContactNil":      {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Name: getRandName()},
		"InvalidContact1": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@test@test.com", Name: getRandName()},
		"InvalidContact2": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@testcom", Name: getRandName()},
		"InvalidContact3": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "testtest.com", Name: getRandName()},
		"InvalidContact4": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "@test.com", Name: getRandName()},
		"InvalidContact5": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@", Name: getRandName()},
		"InvalidContact6": {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@t est.com", Name: getRandName()},
	}

	err := tools.Error{Error: errKsContact, Message: errKsContact}

	for test, ks := range cases {
		testKeyspaceCreationFail(ks.Marshal(), ks.Name, err, test, t)
	}
}

func TestKeyspaceCreateWithConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, resp, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	errConflict := tools.Error{
		Error:   "Cannot create because keyspace \"" + data.Name + "\" already exists",
		Message: "Cannot create because keyspace \"" + data.Name + "\" already exists",
	}

	assert.Equal(t, 409, code)
	assert.Equal(t, errConflict, respErr)
}

func TestKeyspaceCreateInvalidRFString(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "docker-dc",
		"replicationFactor": "a",
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidRFFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rf := "1.1"
	data := `{
		"datacenter": "docker-dc",
		"replicationFactor": ` + rf + `,
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + rf + " into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidTTLString(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "docker-dc",
		"replicationFactor": 1,
		"ttl": "a",
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.ttl of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidTTLFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ttl := "9.1"
	data := `{
		"datacenter": "docker-dc",
		"replicationFactor": 1,
		"ttl": ` + ttl + `,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + ttl + " into Go struct field Config.ttl of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, "", t)
}

func TestKeyspaceCreateNilPayload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	respErr := tools.Error{
		Error:   "EOF",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail(nil, getRandName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidPayload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "docker-dc",
		"replicationFactor": 1,
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	`
	var respErr = tools.Error{
		Error:   "unexpected EOF",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, "", t)
}

// EDIT

func TestKeyspaceEditSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	// name

	dataEdit := tools.KeyspaceEdit{
		Name:    "edit_" + data.Name,
		Contact: data.Contact,
	}
	testKeyspaceEdition(data.ID, dataEdit, t)

	// contact

	dataEdit = tools.KeyspaceEdit{
		Name:    data.Name,
		Contact: "test2edit@domain.com",
	}
	testKeyspaceEdition(data.ID, dataEdit, t)
}

func TestKeyspaceEditConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	dataEdit1 := tools.KeyspaceEdit{
		Name:    "edit1_" + getRandName(),
		Contact: data.Contact,
	}

	dataEdit2 := tools.KeyspaceEdit{
		Name:    "edit2_" + getRandName(),
		Contact: data.Contact,
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go testKeyspaceEditionConcurrently(data.ID, dataEdit1, dataEdit2, t, &wg)
	go testKeyspaceEditionConcurrently(data.ID, dataEdit2, dataEdit1, t, &wg)

	wg.Wait()
}

func TestKeyspaceEditFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	// conflict name
	data2 := getKeyspace()
	testKeyspaceCreation(&data2, t)

	dataEdit := tools.KeyspaceEdit{
		Name:    data2.Name,
		Contact: data.Contact,
	}

	err := tools.Error{
		Error:   "Cannot update because keyspace \"" + data2.Name + "\" already exists",
		Message: "Cannot update because keyspace \"" + data2.Name + "\" already exists",
	}

	testKeyspaceEditionFail(data.ID, dataEdit.Marshal(), 409, err, "", t)

	// empty payload
	err = tools.Error{Error: "EOF", Message: "Wrong JSON format"}

	testKeyspaceEditionFail(data.ID, nil, 400, err, "", t)

	// invalid contact
	casesContact := map[string]tools.KeyspaceEdit{
		"InvalidContact1": {Name: data.Name, Contact: "test@test@test.com"},
		"InvalidContact2": {Name: data.Name, Contact: "test@testcom"},
		"InvalidContact3": {Name: data.Name, Contact: "testtest.com"},
		"InvalidContact4": {Name: data.Name, Contact: "@test.com"},
		"InvalidContact5": {Name: data.Name, Contact: "test@"},
		"InvalidContact6": {Name: data.Name, Contact: "test@t est.com"},
		"InvalidContact7": {Name: data.Name, Contact: ""},
		"InvalidContact8": {Name: data.Name},
	}

	err = tools.Error{Error: errKsContact, Message: errKsContact}

	for test, dataCase := range casesContact {
		testKeyspaceEditionFail(data.ID, dataCase.Marshal(), 400, err, test, t)
	}

	// invalid name
	casesName := map[string]tools.KeyspaceEdit{
		"InvalidName1": {Name: "test_*123", Contact: data.Contact},
		"InvalidName2": {Name: "_test", Contact: data.Contact},
		"InvalidName3": {Name: "", Contact: data.Contact},
		"InvalidName4": {Contact: data.Contact},
	}

	err = tools.Error{Error: errKsName, Message: errKsName}

	for test, dataCase := range casesName {
		testKeyspaceEditionFail(data.ID, dataCase.Marshal(), 400, err, test, t)
	}
}

func TestKeyspaceEditNotExist(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := tools.KeyspaceEdit{
		Name:    fmt.Sprintf("not_exists_%s", getRandName()),
		Contact: "not@exists.com",
	}

	name := "whateverID"

	path := fmt.Sprintf("keyspaces/%s", name)
	code, resp, err := mycenaeTools.HTTP.PUT(path, data.Marshal())
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 404, code)
	assert.Empty(t, resp)
}

// LIST

func TestKeyspaceList(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	path := fmt.Sprintf("keyspaces")
	code, content, err := mycenaeTools.HTTP.GET(path)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.NotContains(t, string(content), `"key":"mycenae"`)
}
