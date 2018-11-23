package solar

import (
	"strconv"

	"github.com/uol/go-solr/solr"
)

/**
* Contains all search related functions.
* @author rnojiri
**/

// buildBasicQuery - builds a basic query
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

// builFilteredQuery - builds a basic query
func (ss *SolrService) buildFilteredQuery(collection, query, fields string, start, rows int, filterQueries []string) *solr.Query {

	q := ss.buildBasicQuery(collection, query, fields, start, rows)

	if filterQueries != nil && len(filterQueries) > 0 {
		for _, fq := range filterQueries {
			q.FilterQuery(fq)
		}
	}

	return q
}

// SimpleQuery - queries the solr
func (ss *SolrService) SimpleQuery(collection, query, fields string, start, rows int) (*solr.SolrResult, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	q := ss.buildBasicQuery(collection, query, fields, start, rows)
	s := si.Search(q)
	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// FilteredQuery - queries the solr
func (ss *SolrService) FilteredQuery(collection, query, fields string, start, rows int, filterQueries []string) (*solr.SolrResult, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	q := ss.buildFilteredQuery(collection, query, fields, start, rows, filterQueries)
	s := si.Search(q)
	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// addFacets - add facets to the query
func (ss *SolrService) addFacets(q *solr.Query, facetFields []string) {

	if facetFields != nil && len(facetFields) > 0 {
		for _, facetField := range facetFields {
			q.AddFacet(facetField)
		}
	}
}

// addChildrenFacets - add facets to the query
func (ss *SolrService) addChildrenFacets(q *solr.Query, facetFields []string) {

	if facetFields != nil && len(facetFields) > 0 {
		for _, facetField := range facetFields {
			q.AddChildFacet(facetField)
		}
	}
}

// Facets - get facets from solr
func (ss *SolrService) Facets(collection, query, fields string, start, rows int, filterQueries []string, facetFields, childrenFacetFields []string, blockJoin bool, facetLimit, minCount int) (*solr.SolrResult, error) {

	si, err := ss.getSolrInterface(collection)
	if err != nil {
		return nil, err
	}

	q := ss.buildFilteredQuery(collection, query, fields, start, rows, filterQueries)
	ss.addFacets(q, facetFields)
	ss.addChildrenFacets(q, childrenFacetFields)
	q.SetFacetMinCount(minCount)
	q.SetParam("facet.limit", strconv.Itoa(facetLimit))

	s := si.Search(q)

	var r *solr.SolrResult
	if blockJoin {
		r, err = s.BlockJoinFaceting(nil)
	} else {
		r, err = s.Result(nil)
	}

	if err != nil {
		return nil, err
	}

	return r, nil
}
