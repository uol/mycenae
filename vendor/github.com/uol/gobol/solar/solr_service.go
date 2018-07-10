package solar

import (
	"errors"
	"fmt"
	"net/url"

	"go.uber.org/zap/zapcore"

	"github.com/uol/go-solr/solr"
	"go.uber.org/zap"
)

/**
* Contains all main structs and functions.
* @author rnojiri
**/

// SolrService - struct
type SolrService struct {
	solrCollectionsAdmin *solr.CollectionsAdmin
	logger               *zap.Logger
	url                  string
	solrInterfaceCache   map[string]*solr.SolrInterface
}

// NewSolrService - creates a new instance
func NewSolrService(url string, logger *zap.Logger) (*SolrService, error) {

	sca, err := solr.NewCollectionsAdmin(url)
	if err != nil {
		return nil, err
	}

	return &SolrService{
		solrCollectionsAdmin: sca,
		logger:               logger,
		url:                  url,
		solrInterfaceCache:   map[string]*solr.SolrInterface{},
	}, nil
}

// getSolrInterface - creates a new solr interface based on the given collection
func (ss *SolrService) getSolrInterface(collection string) (*solr.SolrInterface, error) {

	if si, ok := ss.solrInterfaceCache[collection]; ok {
		return si, nil
	}

	si, err := solr.NewSolrInterface(ss.url, collection)
	if err != nil {
		lf := []zapcore.Field{
			zap.String("package", "solar"),
			zap.String("func", "getSolrInterface"),
		}
		ss.logger.Error("error creating a new instance of solr interface", lf...)
		return nil, err
	}

	ss.solrInterfaceCache[collection] = si

	return si, err
}

// AddDocuments - add one or more documentos to the solr collection
func (ss *SolrService) AddDocuments(collection string, commit bool, docs ...solr.Document) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "AddDocuments"),
	}

	if docs == nil || len(docs) == 0 {
		return errors.New("no documents to add")
	}

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		ss.logger.Error("error getting solr interface", lf...)
		return err
	}

	params := &url.Values{}
	if commit {
		params.Add("commit", "true")
	}

	numDocs := len(docs)
	ss.logger.Info(fmt.Sprintf("adding %d documents to the collection %s", numDocs, collection), lf...)

	_, err = si.Add(docs, 0, params)
	if err != nil {
		ss.logger.Error(fmt.Sprintf("error adding %d document to the collection %s: %s", numDocs, collection, err.Error()), lf...)
		return err
	}

	ss.logger.Info(fmt.Sprintf("added %d documents to the collection %s", numDocs, collection), lf...)

	return nil
}

// DeleteDocumentByID - delete a document by ID
func (ss *SolrService) DeleteDocumentByID(collection string, commit bool, id string) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "DeleteDocumentByID"),
	}

	if id == "" {
		return errors.New("document id not informed, no document will be deleted")
	}

	query := fmt.Sprintf("id:%s", id)

	err := ss.DeleteDocumentByQuery(collection, commit, query)
	if err != nil {
		ss.logger.Error(fmt.Sprintf("error deleting document %s of collection %s: %s", id, collection, err.Error()), lf...)
		return err
	}

	return nil
}

// DeleteDocumentByQuery - delete document by query
func (ss *SolrService) DeleteDocumentByQuery(collection string, commit bool, query string) error {

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "DeleteDocumentByQuery"),
	}

	if query == "" {
		return errors.New("query not informed, no document will be deleted")
	}

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		ss.logger.Error("error getting solr interface", lf...)
		return err
	}

	params := &url.Values{}
	if commit {
		params.Add("commit", "true")
	}

	doc := map[string]interface{}{}
	doc["query"] = query

	_, err = si.Delete(doc, params)
	if err != nil {
		ss.logger.Error(fmt.Sprintf("error deleting document of collection %s with query %s: %s", collection, query, err.Error()), lf...)
		return err
	}

	ss.logger.Info(fmt.Sprintf("deleted document(s) of collection %s with query %s", collection, query), lf...)

	return nil
}
