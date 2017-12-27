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
		Name:              tools.GenerateRandomName(),
		Datacenter:        datacenter,
		ReplicationFactor: 1,
		Contact:           fmt.Sprintf("test-%d@domain.com", time.Now().Unix()),
		TTL:               1,
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

	assert.Equal(t, 1, mycenaeTools.Cassandra.Timeseries.CountTsKeyspaceByKsid(data.Name))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.Name), fmt.Sprintf("Keyspace %v was not created", data.Name))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.ExistsInformation(data.Name, data.ReplicationFactor, data.Datacenter, data.Contact), "Keyspace information was not stored")
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

func testKeyspaceEdition(name, contact string, t *testing.T) {

	ks := tools.KeyspaceUpdate{
		Contact: contact,
	}

	path := fmt.Sprintf("keyspaces/%s", name)
	code, _, err := mycenaeTools.HTTP.PUT(path, ks.Marshal())
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(name)
	assert.Equal(t, contact, ksAfter.Contact)
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
	var caching = map[string]string{
		"keys":               "ALL",
		"rows_per_partition": "ALL",
	}
	var compaction = map[string]string{
		"class":                "DateTieredCompactionStrategy",
		"timestamp_resolution": "SECONDS",
		"base_time_seconds":    "3600",
		"max_sstable_age_days": "180",
	}
	var compression = map[string]string{
		"crc_check_chance":    "0.250000",
		"sstable_compression": "org.apache.cassandra.io.compress.LZ4Compressor",
		"chunk_length_kb":     "1",
	}

	var replication = map[string]string{
		"class":    "NetworkTopologyStrategy",
		datacenter: fmt.Sprintf("%v", data.ReplicationFactor),
	}

	ksProperties := mycenaeTools.Cassandra.Timeseries.KeyspaceProperties(data.Name)
	assert.Exactly(t, replication, ksProperties.Replication)
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.Name), "Keyspace was not created")

	for _, table := range tables {

		tableProperties := mycenaeTools.Cassandra.Timeseries.TableProperties(data.Name, table)

		assert.Exactly(t, 0.01, tableProperties.Bloom_filter_fp_chance)
		assert.Exactly(t, caching, tableProperties.Caching)
		assert.Exactly(t, "", tableProperties.Comment)
		assert.Exactly(t, compaction, tableProperties.Compaction)
		assert.Exactly(t, compression, tableProperties.Compression)
		assert.Exactly(t, 0.0, tableProperties.Dclocal_read_repair_chance)
		assert.Exactly(t, 86400, tableProperties.Default_time_to_live)
		assert.Exactly(t, 0, tableProperties.Gc_grace_seconds)
		assert.Exactly(t, 2048, tableProperties.Max_index_interval)
		assert.Exactly(t, 0, tableProperties.Memtable_flush_period_in_ms)
		assert.Exactly(t, 128, tableProperties.Min_index_interval)
		assert.Exactly(t, 0.0, tableProperties.Read_repair_chance)
		assert.Exactly(t, "99.0PERCENTILE", tableProperties.Speculative_retry)
	}

	keyspaceCassandraTables := mycenaeTools.Cassandra.Timeseries.KeyspaceTables(data.Name)
	sort.Strings(keyspaceCassandraTables)
	sort.Strings(tables)
	assert.Equal(t, tables, keyspaceCassandraTables)
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
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"DCNil":      {Datacenter: "", ReplicationFactor: rf, Contact: contact, Name: tools.GenerateRandomName(), TTL: 1},
		"EmptyDC":    {ReplicationFactor: rf, Contact: contact, Name: tools.GenerateRandomName(), TTL: 1},
		"DCNotExist": {Datacenter: "dc_error", ReplicationFactor: rf, Contact: contact, Name: tools.GenerateRandomName(), TTL: 1},
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
		dc      = datacenter
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"BadName*": {Datacenter: dc, ReplicationFactor: rf, Contact: contact, Name: "test_*123"},
		"_BadName": {Datacenter: dc, ReplicationFactor: rf, Contact: contact, Name: "_test123"},
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
		dc      = datacenter
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]tools.Keyspace{
		"RF0":        {Datacenter: dc, ReplicationFactor: 0, Contact: contact, Name: tools.GenerateRandomName()},
		"RF4":        {Datacenter: dc, ReplicationFactor: 4, Contact: contact, Name: tools.GenerateRandomName()},
		"RFNil":      {Datacenter: dc, Contact: contact, Name: tools.GenerateRandomName()},
		"NegativeRF": {Datacenter: dc, ReplicationFactor: -1, Contact: contact, Name: tools.GenerateRandomName()},
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
		dc      = "dc_gt_a1"
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
		dc  = datacenter
	)

	cases := map[string]tools.Keyspace{
		"ContactNil":      {Datacenter: dc, ReplicationFactor: rf, Name: tools.GenerateRandomName()},
		"InvalidContact1": {Datacenter: dc, ReplicationFactor: rf, Contact: "test@test@test.com", Name: tools.GenerateRandomName()},
		"InvalidContact2": {Datacenter: dc, ReplicationFactor: rf, Contact: "test@testcom", Name: tools.GenerateRandomName()},
		"InvalidContact3": {Datacenter: dc, ReplicationFactor: rf, Contact: "testtest.com", Name: tools.GenerateRandomName()},
		"InvalidContact4": {Datacenter: dc, ReplicationFactor: rf, Contact: "@test.com", Name: tools.GenerateRandomName()},
		"InvalidContact5": {Datacenter: dc, ReplicationFactor: rf, Contact: "test@", Name: tools.GenerateRandomName()},
		"InvalidContact6": {Datacenter: dc, ReplicationFactor: rf, Contact: "test@t est.com", Name: tools.GenerateRandomName()},
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
		"datacenter": "dc_gt_a1",
		"replicationFactor": "a",
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), tools.GenerateRandomName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidRFFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rf := "1.1"
	data := `{
		"datacenter": "dc_gt_a1",
		"replicationFactor": ` + rf + `,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + rf + " into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), tools.GenerateRandomName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidTTLString(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "dc_gt_a1",
		"replicationFactor": 1,
		"ttl": "x",
		"contact": "` + fmt.Sprintf("test-%d@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.ttl of type uint8",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), tools.GenerateRandomName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidTTLFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ttl := "9.1"
	data := `{
		"datacenter": "` + datacenter + `",
		"replicationFactor": 1,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `",
		"ttl": ` + ttl + `
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + ttl + " into Go struct field Config.ttl of type uint8",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), tools.GenerateRandomName(), respErr, "", t)
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

	testKeyspaceCreationFail(nil, tools.GenerateRandomName(), respErr, "", t)
}

func TestKeyspaceCreateInvalidPayload(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "` + datacenter + `",
		"replicationFactor": 1,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	`
	var respErr = tools.Error{
		Error:   "unexpected EOF",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), tools.GenerateRandomName(), respErr, "", t)
}

// EDIT

func TestKeyspaceEditSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	testKeyspaceEdition(data.Name, data.Contact, t)
}

func TestKeyspaceEditFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	// empty payload
	err := tools.Error{Error: "EOF", Message: "Wrong JSON format"}

	testKeyspaceEditionFail(data.Name, nil, 400, err, "", t)

	// invalid contact
	casesContact := map[string]string{
		"InvalidContact1": "test@test@test.com",
		"InvalidContact2": "test@testcom",
		"InvalidContact3": "testtest.com",
		"InvalidContact4": "@test.com",
		"InvalidContact5": "test@",
		"InvalidContact6": "test@t est.com",
		"InvalidContact7": "",
	}

	err = tools.Error{Error: errKsContact, Message: errKsContact}

	ksData := tools.KeyspaceUpdate{}

	for test, dataCase := range casesContact {
		ksData.Contact = dataCase
		testKeyspaceEditionFail(data.Name, ksData.Marshal(), 400, err, test, t)
	}
}

func TestKeyspaceEditNotExist(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	name := "whateverID"

	cu := keyspace.ConfigUpdate{Contact: "not@exists.com"}
	json, _ := json.Marshal(&cu)

	path := fmt.Sprintf("keyspaces/%s", name)
	code, resp, err := mycenaeTools.HTTP.PUT(path, json)
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
