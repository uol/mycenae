package solar

import (
	"github.com/uol/go-solr/solr"
)

/**
* Contains all search related functions.
* @author rnojiri
**/

//buildBasicQuery - builds a basic query
func (ss *SolrService) buildBasicQuery(collection, query, fields string, start, rows int) *solr.Query {

	q := solr.NewQuery()
	q.Q(query)

	if fields != "" {
		q.FieldList(fields)
	}

	q.Start(start)
	q.Rows(rows)

	return q
}

//SimpleQuery - queries the solr
func (ss *SolrService) SimpleQuery(collection, query, fields string, start, rows int) (*solr.SolrResult, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	q := ss.buildBasicQuery(collection, query, fields, start, rows)
	s := si.Search(q)
	r, _ := s.Result(nil)

	return r, nil
}

//Facet - facets the solr
func (ss *SolrService) Facets(collection, query, fields string, start, rows int, facets ...string) (*solr.SolrResult, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	q := ss.buildBasicQuery(collection, query, fields, start, rows)
	if facets != nil && len(facets) > 0 {
		for _, facet := range facets {
			q.AddFacet(facet)
		}
	}

	s := si.Search(q)
	r, _ := s.Result(nil)

	return r, nil
}
