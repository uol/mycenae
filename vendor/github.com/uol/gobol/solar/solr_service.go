package solar

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

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
	solrInterfaceCache   sync.Map
}

// recoverFromFailure - recovers from a failure
func (ss *SolrService) recoverFromFailure(funcName string) {
	if r := recover(); r != nil {
		lf := []zapcore.Field{
			zap.String("package", "solar"),
			zap.String("func", funcName),
		}
		ss.logger.Error(fmt.Sprintf("recovered from: %s", r), lf...)
	}
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
		solrInterfaceCache:   sync.Map{},
	}, nil
}

// getSolrInterface - creates a new solr interface based on the given collection
func (ss *SolrService) getSolrInterface(collection string) (*solr.SolrInterface, error) {

	if si, ok := ss.solrInterfaceCache.Load(collection); ok {
		return si.(*solr.SolrInterface), nil
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

	ss.solrInterfaceCache.Store(collection, si)

	return si, err
}

// AddDocument - add one document to the solr collection
func (ss *SolrService) AddDocument(collection string, commit bool, doc *solr.Document) error {

	defer ss.recoverFromFailure("AddDocuments")

	lf := []zapcore.Field{
		zap.String("package", "solar"),
		zap.String("func", "AddDocuments"),
	}

	if doc == nil {
		return errors.New("document is null")
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

	_, err = si.Add([]solr.Document{*doc}, 0, params)
	if err != nil {
		ss.logger.Error(fmt.Sprintf("error adding 1 document to the collection %s: %s", collection, err.Error()), lf...)
		return err
	}

	ss.logger.Info(fmt.Sprintf("added 1 documents to the collection %s", collection), lf...)

	return nil
}

// AddDocuments - add one or more documentos to the solr collection
func (ss *SolrService) AddDocuments(collection string, commit bool, docs ...solr.Document) error {

	defer ss.recoverFromFailure("AddDocuments")

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

	defer ss.recoverFromFailure("DeleteDocumentByID")

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

	defer ss.recoverFromFailure("DeleteDocumentByQuery")

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
