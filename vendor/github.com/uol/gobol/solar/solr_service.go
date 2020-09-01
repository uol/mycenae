package solar

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/uol/restrictedhttpclient"

	"github.com/uol/funks"

	"github.com/uol/go-solr/solr"
	"github.com/uol/logh"
)

/**
* Contains all main structs and functions.
* @author rnojiri
**/

// SolrService - struct
type SolrService struct {
	solrCollectionsAdmin *solr.CollectionsAdmin
	loggers              *logh.ContextualLogger
	url                  string
	solrInterfaceCache   sync.Map
	queryClient          *restrictedhttpclient.Instance
	updateClient         *restrictedhttpclient.Instance
}

// recoverFromFailure - recovers from a failure
func (ss *SolrService) recoverFromFailure() {
	if r := recover(); r != nil {
		if logh.ErrorEnabled {
			ss.loggers.Error().Msg(fmt.Sprintf("recovered from: %s", r))
		}
	}
}

// HTTPClient - the http client configuration
type HTTPClient struct {
	Timeout                    funks.Duration
	NumSimultaneousConnections int
}

// Configuration - the configuration
type Configuration struct {
	URL          string
	QueryClient  *restrictedhttpclient.Configuration
	UpdateClient *restrictedhttpclient.Configuration
}

// NewSolrService - creates a new instance
func NewSolrService(configuration *Configuration) (*SolrService, error) {

	if configuration == nil {
		return nil, errors.New("null configuration")
	}

	queryClient, err := restrictedhttpclient.New(configuration.QueryClient)
	if err != nil {
		return nil, err
	}

	updateClient, err := restrictedhttpclient.New(configuration.UpdateClient)
	if err != nil {
		return nil, err
	}

	sca, err := solr.NewCollectionsAdmin(configuration.URL, queryClient)
	if err != nil {
		return nil, err
	}

	return &SolrService{
		solrCollectionsAdmin: sca,
		loggers:              logh.CreateContextualLogger("pkg", "solar"),
		url:                  configuration.URL,
		solrInterfaceCache:   sync.Map{},
		queryClient:          queryClient,
		updateClient:         updateClient,
	}, nil
}

// getSolrInterface - creates a new solr interface based on the given collection
func (ss *SolrService) getSolrInterface(collection string) (*solr.SolrInterface, error) {

	if si, ok := ss.solrInterfaceCache.Load(collection); ok {
		return si.(*solr.SolrInterface), nil
	}

	si, err := solr.NewSolrInterface(ss.url, collection, ss.queryClient, ss.updateClient)
	if err != nil {
		if logh.ErrorEnabled {
			ss.loggers.Error().Err(err).Msg("error creating a new instance of solr interface")
		}
		return nil, err
	}

	ss.solrInterfaceCache.Store(collection, si)

	return si, err
}

const (
	cCommit string = "commit"
	cTrue   string = "true"
	cEmpty  string = ""
	cQuery  string = "query"
)

// AddDocument - add one document to the solr collection
func (ss *SolrService) AddDocument(collection string, commit bool, doc *solr.Document) error {

	defer ss.recoverFromFailure()

	if doc == nil {
		return errors.New("document is null")
	}

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		if logh.ErrorEnabled {
			ss.loggers.Error().Err(err).Msg("error getting solr interface")
		}
		return err
	}

	params := &url.Values{}
	if commit {
		params.Add(cCommit, cTrue)
	}

	_, err = si.Add([]solr.Document{*doc}, 0, params)
	if err != nil {
		return err
	}

	return nil
}

// AddDocuments - add one or more documentos to the solr collection
func (ss *SolrService) AddDocuments(collection string, commit bool, docs ...solr.Document) error {

	defer ss.recoverFromFailure()

	if docs == nil || len(docs) == 0 {
		return errors.New("no documents to add")
	}

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		if logh.ErrorEnabled {
			ss.loggers.Error().Err(err).Msg("error getting solr interface")
		}
		return err
	}

	params := &url.Values{}
	if commit {
		params.Add(cCommit, cTrue)
	}

	_, err = si.Add(docs, 0, params)
	if err != nil {
		return err
	}

	return nil
}

// DeleteDocumentByID - delete a document by ID
func (ss *SolrService) DeleteDocumentByID(collection string, commit bool, id string) error {

	defer ss.recoverFromFailure()

	if id == cEmpty {
		return errors.New("document id not informed, no document will be deleted")
	}

	query := fmt.Sprintf("id:%s", id)

	err := ss.DeleteDocumentByQuery(collection, commit, query)
	if err != nil {
		return err
	}

	return nil
}

// DeleteDocumentByQuery - delete document by query
func (ss *SolrService) DeleteDocumentByQuery(collection string, commit bool, query string) error {

	defer ss.recoverFromFailure()

	if query == cEmpty {
		return errors.New("query not informed, no document will be deleted")
	}

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		if logh.ErrorEnabled {
			ss.loggers.Error().Err(err).Msg("error getting solr interface")
		}
		return err
	}

	params := &url.Values{}
	if commit {
		params.Add(cCommit, cTrue)
	}

	doc := map[string]interface{}{}
	doc[cQuery] = query

	solrResponse, err := si.Delete(doc, params)
	if err != nil {
		return err
	}

	if !solrResponse.Success {
		return fmt.Errorf("error deleting documents: %+v", solrResponse.Result)
	}

	return nil
}
