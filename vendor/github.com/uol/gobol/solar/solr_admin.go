package solar

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/uol/go-solr/solr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/**
* Contains all administrative functions.
* @author rnojiri
**/

// Field - a solr schema field
type Field struct {
	Name        string `json:"name"`
	FieldType   string `json:"type"`
	MultiValued bool   `json:"multiValued"`
	Stored      bool   `json:"stored"`
	Indexed     bool   `json:"indexed"`
	DocValues   bool   `json:"docValues"`
}

// AddField - the json struct to add a field to solr schema
type AddField struct {
	Field Field `json:"add-field"`
}

// AddNewField - adds a new field to solr schema
func (ss *SolrService) AddNewField(collection, name, fieldType string, multiValued, stored, indexed, docValues bool) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "AddNewField"),
	}

	schema, err := ss.getSchema(collection)
	if err != nil {
		return err
	}

	data := AddField{
		Field: Field{
			Name:        name,
			FieldType:   fieldType,
			MultiValued: multiValued,
			Stored:      stored,
			Indexed:     indexed,
			DocValues:   docValues,
		},
	}

	ss.logger.Info(fmt.Sprintf("adding new field to %s: %s", collection, name), lf...)

	_, err = schema.Post("", data)
	if err != nil {
		msg := fmt.Sprintf("error adding field %s", name)
		ss.logger.Error(msg, lf...)

		if err != nil {
			return err
		}

		return errors.New(msg)
	}

	ss.logger.Info(fmt.Sprintf("new field added to collection %s: %s", collection, name), lf...)

	return nil
}

// getSchema - returns the schema instance from a collection
func (ss *SolrService) getSchema(collection string) (*solr.Schema, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	schema, err := si.Schema()
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "solar"),
			zap.String("func", "getSchema"),
		}
		ss.logger.Error("error creating a new schema instance", lf...)
		return nil, err
	}

	return schema, nil
}

// CreateCollection - creates a new collection
func (ss *SolrService) CreateCollection(collection string, numShards, replicationFactor int) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "CreateCollection"),
	}

	params := &url.Values{}
	params.Add("name", collection)
	params.Add("numShards", strconv.Itoa(numShards))
	params.Add("replicationFactor", strconv.Itoa(replicationFactor))
	params.Add("waitForFinalState", "true")

	ss.logger.Info(fmt.Sprintf("creating collection: %s", collection), lf...)

	r, err := ss.solrCollectionsAdmin.Action("CREATE", params)
	if err != nil {
		return err
	}
	if r.Status != 0 {
		ss.logger.Error(fmt.Sprintf("received a non ok status: %d", r.Status), lf...)
		return errors.New("collection creation failed")
	}

	ss.logger.Info(fmt.Sprintf("collection created: %s", collection), lf...)

	return nil
}

// DeleteCollection - deletes a collection
func (ss *SolrService) DeleteCollection(collection string) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "DeleteCollection"),
	}

	ss.logger.Info(fmt.Sprintf("deleting collection: %s", collection), lf...)

	params := &url.Values{}
	params.Add("name", collection)
	r, err := ss.solrCollectionsAdmin.Action("DELETE", params)
	if err != nil {
		return err
	}
	if r.Status != 0 {
		ss.logger.Error(fmt.Sprintf("received a non ok status: %d", r.Status), lf...)
		return errors.New("collection remove failed")
	}

	ss.logger.Info(fmt.Sprintf("collection deleted: %s", collection), lf...)

	return nil
}

// ListCollections - list all collections
func (ss *SolrService) ListCollections() ([]string, error) {

	r, err := ss.solrCollectionsAdmin.Action("LIST", nil)
	if err != nil {
		return nil, err
	}
	if r.Status != 0 {
		lf := []zapcore.Field{
			zap.String("package", "solar"),
			zap.String("func", "ListCollections"),
			zap.String("step", "Action"),
		}
		ss.logger.Error(fmt.Sprintf("received a non ok status: %d", r.Status), lf...)
		return nil, errors.New("list collections failed")
	}

	collections := []string{}
	for _, item := range r.Response["collections"].([]interface{}) {
		collections = append(collections, item.(string))
	}

	return collections, nil
}
