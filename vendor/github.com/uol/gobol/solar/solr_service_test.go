package solar_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/uol/go-solr/solr"

	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"

	"github.com/uol/gobol/saw"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const solrUrl = "http://172.17.0.2:8983/solr"

// getLogger - creates the logger
func getLogger(t *testing.T) (*zap.Logger, error) {

	logger, err := saw.New("DEBUG", "QA")
	if err != nil {
		t.Errorf(err.Error())
		t.Fail()
	}

	return logger, err
}

// RandStringBytes - generates random strings
func randStringBytes(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// initSolrService - initializes the solr service
func initSolrService(t *testing.T) *SolrService {

	logger, _ := getLogger(t)

	ss, err := NewSolrService(solrUrl, logger)
	if err != nil {
		t.Errorf(err.Error())
		panic(err)
	}

	return ss
}

// createCollection - creates a new collection using a random name
func createCollection(t *testing.T, ss *SolrService) (string, error) {

	collection := randStringBytes(10)
	err := ss.CreateCollection(collection, 1, 2)
	if err != nil {
		t.Errorf(err.Error())
		return "", err
	}

	return collection, nil
}

// collectionExists - checks if a collection exists on solr
func collectionExists(t *testing.T, ss *SolrService, collection string) (bool, error) {
	collections, err := ss.ListCollections()
	if err != nil {
		t.Errorf(err.Error())
		return false, err
	}

	found := false
	for _, item := range collections {
		if collection == item {
			found = true
			break
		}
	}

	return found, nil
}

func TestCollectionCreation(t *testing.T) {

	ss := initSolrService(t)

	collection, err := createCollection(t, ss)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	found, err := collectionExists(t, ss, collection)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	assert.True(t, found)
}

func TestCollectionDeletion(t *testing.T) {

	ss := initSolrService(t)

	collection, err := createCollection(t, ss)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	found, err := collectionExists(t, ss, collection)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if !found {
		t.Fail()
	} else {
		err := ss.DeleteCollection(collection)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}

	found, err = collectionExists(t, ss, collection)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	assert.False(t, found)
}

// getBodyContent - returns the body content from the response
func getBodyContent(t *testing.T, resp *http.Response) (string, error) {
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	if resp.StatusCode == http.StatusOK {

		defer resp.Body.Close()

		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf(err.Error())
			return "", err
		}

		bodyString := string(bodyBytes)

		return bodyString, nil
	} else {
		return "", errors.New(fmt.Sprintf("unexpected status code: %d", resp.StatusCode))
	}
}

func TestCollectionSchema(t *testing.T) {

	ss := initSolrService(t)

	collection, err := createCollection(t, ss)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	err = ss.AddNewField(collection, "string_field", "string", false, true, true, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	err = ss.AddNewField(collection, "boolean_field", "boolean", false, true, true, false)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	err = ss.AddNewField(collection, "string_array_field", "string", true, true, true, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	err = ss.AddNewField(collection, "boolean_array_field", "boolean", true, true, true, false)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/%s/schema/fields?indent=off&wt=json", solrUrl, collection))
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	bodyString, err := getBodyContent(t, resp)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	assert.Contains(t, bodyString, "\"name\":\"string_field\",\"type\":\"string\",\"docValues\":true,\"multiValued\":false")
	assert.Contains(t, bodyString, "\"name\":\"boolean_field\",\"type\":\"boolean\",\"docValues\":false,\"multiValued\":false")
	assert.Contains(t, bodyString, "\"name\":\"string_array_field\",\"type\":\"string\",\"docValues\":true,\"multiValued\":true")
	assert.Contains(t, bodyString, "\"name\":\"boolean_array_field\",\"type\":\"boolean\",\"docValues\":false,\"multiValued\":true")
}

func TestAddSingleDocument(t *testing.T) {

	ss := initSolrService(t)

	collection, err := createCollection(t, ss)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	doc := solr.Document{
		"id": "1",
	}

	err = ss.AddDocuments(collection, true, doc)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/%s/select?indent=off&wt=json&q=*:*", solrUrl, collection))
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	bodyString, err := getBodyContent(t, resp)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	assert.Contains(t, bodyString, "\"numFound\":1")
}

func TestAddMultipleDocuments(t *testing.T) {

	ss := initSolrService(t)

	collection, err := createCollection(t, ss)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	docs := make([]solr.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = solr.Document{
			"id": strconv.Itoa(i),
		}
	}

	err = ss.AddDocuments(collection, true, docs...)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/%s/select?indent=off&wt=json&q=*:*", solrUrl, collection))
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	bodyString, err := getBodyContent(t, resp)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	assert.Contains(t, bodyString, "\"numFound\":10")
}
