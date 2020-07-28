package solr

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CollectionsAdmin - type definition
type CollectionsAdmin struct {
	url      *url.URL
	username string
	password string
	client   *http.Client
}

// NewCollectionsAdmin - creates a new collection api connection
func NewCollectionsAdmin(solrURL string, client *http.Client) (*CollectionsAdmin, error) {
	u, err := url.ParseRequestURI(strings.TrimRight(solrURL, "/"))
	if err != nil {
		return nil, err
	}

	return &CollectionsAdmin{url: u, client: client}, nil
}

// SetBasicAuth - Set basic auth in case solr require login
func (ca *CollectionsAdmin) SetBasicAuth(username, password string) {
	ca.username = username
	ca.password = password
}

// Get - Method for making GET-request to any relative path to /admin/collections
func (ca *CollectionsAdmin) Get(params *url.Values) (*SolrResponse, error) {
	params.Set("wt", "json")
	r, err := HTTPGet(ca.client, fmt.Sprintf("%s/admin/collections?%s", ca.url.String(), params.Encode()), nil, ca.username, ca.password)
	if err != nil {
		return nil, err
	}
	resp, err := bytes2json(&r)
	if err != nil {
		return nil, err
	}
	result := &SolrResponse{Response: resp}
	rHeader, convOk := resp["responseHeader"].(map[string]interface{})
	if !convOk {
		return nil, TypeCastingError
	}

	result.Status = int(rHeader["status"].(float64))

	return result, nil
}

// Action - calls the specified action using the admin/collections endpoint
func (ca *CollectionsAdmin) Action(action string, params *url.Values) (*SolrResponse, error) {
	if params == nil {
		params = &url.Values{}
	}
	switch strings.ToUpper(action) {
	case "CREATE":
		params.Set("action", "CREATE")
	case "MODIFYCOLLECTION":
		params.Set("action", "MODIFYCOLLECTION")
	case "RELOAD":
		params.Set("action", "RELOAD")
	case "SPLITSHARD":
		params.Set("action", "SPLITSHARD")
	case "CREATESHARD":
		params.Set("action", "CREATESHARD")
	case "DELETESHARD":
		params.Set("action", "DELETESHARD")
	case "CREATEALIAS":
		params.Set("action", "CREATEALIAS")
	case "LISTALIASES":
		params.Set("action", "LISTALIASES")
	case "DELETEALIAS":
		params.Set("action", "DELETEALIAS")
	case "DELETE":
		params.Set("action", "DELETE")
	case "DELETEREPLICA":
		params.Set("action", "DELETEREPLICA")
	case "ADDREPLICA":
		params.Set("action", "ADDREPLICA")
	case "CLUSTERPROP":
		params.Set("action", "CLUSTERPROP")
	case "MIGRATE":
		params.Set("action", "MIGRATE")
	case "ADDROLE":
		params.Set("action", "ADDROLE")
	case "REMOVEROLE":
		params.Set("action", "REMOVEROLE")
	case "OVERSEERSTATUS":
		params.Set("action", "OVERSEERSTATUS")
	case "CLUSTERSTATUS":
		params.Set("action", "CLUSTERSTATUS")
	case "REQUESTSTATUS":
		params.Set("action", "REQUESTSTATUS")
	case "DELETESTATUS":
		params.Set("action", "DELETESTATUS")
	case "LIST":
		params.Set("action", "LIST")
	case "ADDREPLICAPROP":
		params.Set("action", "ADDREPLICAPROP")
	case "DELETEREPLICAPROP":
		params.Set("action", "DELETEREPLICAPROP")
	case "BALANCESHARDUNIQUE":
		params.Set("action", "BALANCESHARDUNIQUE")
	case "REBALANCELEADERS":
		params.Set("action", "REBALANCELEADERS")
	case "FORCELEADER":
		params.Set("action", "FORCELEADER")
	case "MIGRATESTATEFORMAT":
		params.Set("action", "MIGRATESTATEFORMAT")
	case "BACKUP":
		params.Set("action", "BACKUP")
	case "RESTORE":
		params.Set("action", "RESTORE")
	case "DELETENODE":
		params.Set("action", "DELETENODE")
	case "REPLACENODE":
		params.Set("action", "REPLACENODE")
	case "MOVEREPLICA":
		params.Set("action", "MOVEREPLICA")
	case "UTILIZENODE":
		params.Set("action", "UTILIZENODE")
	default:
		return nil, fmt.Errorf("Action '%s' not supported", action)
	}
	return ca.Get(params)
}

// CollectionsAdmin - Return new instance of CollectionsAdmin with provided solrUrl and basic auth
func (si *SolrInterface) CollectionsAdmin() (*CollectionsAdmin, error) {
	ca, err := NewCollectionsAdmin(si.conn.url.String(), si.client)
	if err != nil {
		return nil, err
	}
	ca.SetBasicAuth(si.conn.username, si.conn.password)
	return ca, nil
}
