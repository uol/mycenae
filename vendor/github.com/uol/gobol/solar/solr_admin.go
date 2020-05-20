package solar

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/uol/go-solr/solr"
	"github.com/uol/logh"
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

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("adding new field to %s: %s", collection, name))
	}

	_, err = schema.Post("", data)
	if err != nil {
		msg := fmt.Sprintf("error adding field %s", name)

		if logh.ErrorEnabled {
			ss.loggers.Error().Msg(msg)
		}

		if err != nil {
			return err
		}

		return errors.New(msg)
	}

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("new field added to collection %s: %s", collection, name))
	}

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
		if logh.ErrorEnabled {
			ss.loggers.Error().Msg("error retrieving a schema instance")
		}
		return nil, err
	}

	return schema, nil
}

// CreateCollection - creates a new collection
func (ss *SolrService) CreateCollection(collection, configSet string, numShards, replicationFactor int) error {

	params := &url.Values{}
	params.Add("name", collection)
	params.Add("numShards", strconv.Itoa(numShards))
	params.Add("replicationFactor", strconv.Itoa(replicationFactor))
	params.Add("waitForFinalState", "true")

	if configSet != "" {
		params.Add("collection.configName", configSet)
	}

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("creating collection: %s", collection))
	}

	r, err := ss.solrCollectionsAdmin.Action("CREATE", params)
	if err != nil {
		return err
	}
	if r.Status != 0 {
		if logh.ErrorEnabled {
			ss.loggers.Error().Msg(fmt.Sprintf("received a non ok status: %d", r.Status))
		}
		return errors.New("collection creation failed")
	}

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("collection created: %s", collection))
	}

	return nil
}

// DeleteCollection - deletes a collection
func (ss *SolrService) DeleteCollection(collection string) error {

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("deleting collection: %s", collection))
	}

	params := &url.Values{}
	params.Add("name", collection)
	r, err := ss.solrCollectionsAdmin.Action("DELETE", params)
	if err != nil {
		return err
	}
	if r.Status != 0 {
		if logh.ErrorEnabled {
			ss.loggers.Error().Msg(fmt.Sprintf("received a non ok status: %d", r.Status))
		}
		return errors.New("collection remove failed")
	}

	if logh.InfoEnabled {
		ss.loggers.Info().Msg(fmt.Sprintf("collection deleted: %s", collection))
	}

	return nil
}

// ListCollections - list all collections
func (ss *SolrService) ListCollections() ([]string, error) {

	r, err := ss.solrCollectionsAdmin.Action("LIST", nil)
	if err != nil {
		return nil, err
	}
	if r.Status != 0 {
		if logh.ErrorEnabled {
			ss.loggers.Error().Msg(fmt.Sprintf("received a non ok status: %d", r.Status))
		}
		return nil, errors.New("list collections failed")
	}

	collections := []string{}
	for _, item := range r.Response["collections"].([]interface{}) {
		collections = append(collections, item.(string))
	}

	return collections, nil
}
