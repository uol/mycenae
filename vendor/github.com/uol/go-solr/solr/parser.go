package solr

import (
	"encoding/json"
	"fmt"
)

func bytes2json(data *[]byte) (map[string]interface{}, error) {
	var jsonData interface{}

	err := json.Unmarshal(*data, &jsonData)

	if err != nil {
		return nil, err
	}

	jsonDataIf, convOk := jsonData.(map[string]interface{})
	if !convOk {
		return nil, fmt.Errorf("bytes2json - type casting error")
	}

	return jsonDataIf, nil
}

// ResultParser is interface for parsing result from response.
// The idea here is that application have possibility to parse.
// Or defined own parser with internal data structure to suite
// application's need
type ResultParser interface {
	Parse(resp *[]byte) (*SolrResult, error)
}

type FireworkResultParser struct {
}

func (parser *FireworkResultParser) Parse(resp *[]byte) (FireworkSolrResult, error) {
	var res FireworkSolrResult
	err := json.Unmarshal(*resp, &res)
	return res, err
}

type ExtensiveResultParser struct {
}

func (parser *ExtensiveResultParser) Parse(resp_ *[]byte) (*SolrResult, error) {
	sr := &SolrResult{}
	jsonbuf, err := bytes2json(resp_)
	if err != nil {
		return sr, err
	}
	response := new(SolrResponse)
	response.Response = jsonbuf
	rHeader, convOk := jsonbuf["responseHeader"].(map[string]interface{})
	if !convOk {
		return nil, fmt.Errorf("Parse - type casting error")
	}

	response.Status = int(rHeader["status"].(float64))

	sr.Results = new(Collection)
	sr.Status = response.Status
	if nextCursorMark, ok := jsonbuf["nextCursorMark"]; ok {
		sr.NextCursorMark = fmt.Sprintf("%s", nextCursorMark)
	}

	err = parser.ParseResponseHeader(response, sr)
	if err != nil {
		return nil, err
	}

	if 0 != response.Status {
		err := parser.ParseError(response, sr)
		if err != nil {
			return nil, err
		}

		return sr, nil
	}

	err = parser.ParseResponse(response, sr)
	if err != nil {
		return nil, err
	}

	err = parser.ParseFacets(response, sr)
	if err != nil {
		return nil, err
	}

	err = parser.ParseJsonFacets(response, sr)
	if err != nil {
		return nil, err
	}

	return sr, nil
}

func (parser *ExtensiveResultParser) ParseResponseHeader(response *SolrResponse, sr *SolrResult) error {
	if responseHeader, ok := response.Response["responseHeader"].(map[string]interface{}); ok {
		sr.ResponseHeader = responseHeader
		return nil
	}

	return fmt.Errorf("ParseResponseHeader - type casting error")
}

func (parser *ExtensiveResultParser) ParseError(response *SolrResponse, sr *SolrResult) error {
	if err, ok := response.Response["error"].(map[string]interface{}); ok {
		sr.Error = err
		return nil
	}

	return fmt.Errorf("ParseError - type casting error")
}

// ParseJsonFacets will assign facets and build sr.jsonfacets if there is a facet_counts
func (parser *ExtensiveResultParser) ParseFacets(response *SolrResponse, sr *SolrResult) error {
	if fc, ok := response.Response["facet_counts"].(map[string]interface{}); ok {
		sr.FacetCounts = fc
		if f, ok := fc["facet_fields"].(map[string]interface{}); ok {
			sr.Facets = f
		} else {
			return fmt.Errorf("ParseFacets - type casting error (a)")
		}

		return nil
	}

	return fmt.Errorf("ParseFacets - type casting error (b)")
}

// ParseJsonFacets will assign facets and build sr.jsonfacets if there is a facets
func (parser *ExtensiveResultParser) ParseJsonFacets(response *SolrResponse, sr *SolrResult) error {
	if jf, ok := response.Response["facets"].(map[string]interface{}); ok {
		sr.JsonFacets = jf
		return nil
	}

	return fmt.Errorf("ParseJsonFacets - type casting error")
}

// ParseSolrResponse will assign result and build sr.docs if there is a response.
// If there is no response or grouped property in response it will return error
func (parser *ExtensiveResultParser) ParseResponse(response *SolrResponse, sr *SolrResult) (err error) {
	if resp, ok := response.Response["response"].(map[string]interface{}); ok {
		err = ParseDocResponse(resp, sr.Results)
		if err != nil {
			return err
		}
	} else {
		err = fmt.Errorf(`Extensive parser can only parse solr response with response object,
					ie response.response and response.response.docs. Or grouped response
					Please use other parser or implement your own parser`)
	}

	return err
}

type StandardResultParser struct {
}

func (parser *StandardResultParser) Parse(resp_ *[]byte) (*SolrResult, error) {

	sr := &SolrResult{}
	jsonbuf, err := bytes2json(resp_)
	if err != nil {
		return sr, err
	}
	response := new(SolrResponse)
	response.Response = jsonbuf
	rHeader, convOk := jsonbuf["responseHeader"].(map[string]interface{})
	if !convOk {
		return nil, fmt.Errorf("Parse - type casting error")
	}

	response.Status = int(rHeader["status"].(float64))

	sr.Results = new(Collection)
	sr.Status = response.Status
	if jsonbuf["nextCursorMark"] != nil {
		sr.NextCursorMark = fmt.Sprintf("%s", jsonbuf["nextCursorMark"])
	}

	err = parser.ParseResponseHeader(response, sr)
	if err != nil {
		return nil, err
	}

	if response.Status == 0 {
		err := parser.ParseResponse(response, sr)
		if err != nil {
			return nil, err
		}

		parser.ParseFacetCounts(response, sr)
		parser.ParseHighlighting(response, sr)
		parser.ParseStats(response, sr)
		parser.ParseMoreLikeThis(response, sr)
		parser.ParseSpellCheck(response, sr)
	} else {
		err := parser.ParseError(response, sr)
		if err != nil {
			return nil, err
		}
	}

	return sr, nil
}

func (parser *StandardResultParser) ParseResponseHeader(response *SolrResponse, sr *SolrResult) error {
	if responseHeader, ok := response.Response["responseHeader"].(map[string]interface{}); ok {
		sr.ResponseHeader = responseHeader
		return nil
	}

	return fmt.Errorf("ParseResponseHeader - type casting error")
}

func (parser *StandardResultParser) ParseError(response *SolrResponse, sr *SolrResult) error {
	if err, ok := response.Response["error"].(map[string]interface{}); ok {
		sr.Error = err
		return nil
	}

	return fmt.Errorf("ParseError - type casting error")
}

func ParseDocResponse(docResponse map[string]interface{}, collection *Collection) error {
	collection.NumFound = int(docResponse["numFound"].(float64))
	collection.Start = int(docResponse["start"].(float64))
	if docs, ok := docResponse["docs"].([]interface{}); ok {
		collection.Docs = make([]Document, len(docs))
		for i, v := range docs {
			d, convOk := v.(map[string]interface{})
			if !convOk {
				return fmt.Errorf("ParseDocResponse - type casting error (for)")
			}

			collection.Docs[i] = Document(d)
		}
	}

	return nil
}

// ParseSolrResponse will assign result and build sr.docs if there is a response.
// If there is no response or grouped property in response it will return error
func (parser *StandardResultParser) ParseResponse(response *SolrResponse, sr *SolrResult) (err error) {
	if resp, ok := response.Response["response"].(map[string]interface{}); ok {
		err = ParseDocResponse(resp, sr.Results)
		if err != nil {
			return err
		}
	} else if grouped, ok := response.Response["grouped"].(map[string]interface{}); ok {
		sr.Grouped = grouped
	} else {
		err = fmt.Errorf(`Standard parser can only parse solr response with response object,
					ie response.response and response.response.docs. Or grouped response
					Please use other parser or implement your own parser`)
	}

	return err
}

// ParseFacetCounts will assign facet_counts to sr if there is one.
// No modification done here
func (parser *StandardResultParser) ParseFacetCounts(response *SolrResponse, sr *SolrResult) error {
	if facetCounts, ok := response.Response["facet_counts"].(map[string]interface{}); ok {
		sr.FacetCounts = facetCounts
		return nil
	}

	return fmt.Errorf("ParseFacetCounts - type casting error")
}

// ParseHighlighting will assign highlighting to sr if there is one.
// No modification done here
func (parser *StandardResultParser) ParseHighlighting(response *SolrResponse, sr *SolrResult) error {
	if highlighting, ok := response.Response["highlighting"].(map[string]interface{}); ok {
		sr.Highlighting = highlighting
		return nil
	}

	return fmt.Errorf("ParseHighlighting - type casting error")
}

// Parse stats if there is  in response
func (parser *StandardResultParser) ParseStats(response *SolrResponse, sr *SolrResult) error {
	if stats, ok := response.Response["stats"].(map[string]interface{}); ok {
		sr.Stats = stats
		return nil
	}

	return fmt.Errorf("ParseStats - type casting error")
}

// Parse moreLikeThis if there is in response
func (parser *StandardResultParser) ParseMoreLikeThis(response *SolrResponse, sr *SolrResult) error {
	if moreLikeThis, ok := response.Response["moreLikeThis"].(map[string]interface{}); ok {
		sr.MoreLikeThis = moreLikeThis
		return nil
	}

	return fmt.Errorf("ParseMoreLikeThis - type casting error")
}

// Parse moreLikeThis if there is in response
func (parser *StandardResultParser) ParseSpellCheck(response *SolrResponse, sr *SolrResult) error {
	if spellCheck, ok := response.Response["spellcheck"].(map[string]interface{}); ok {
		sr.SpellCheck = spellCheck
		return nil
	}

	return fmt.Errorf("ParseSpellCheck - type casting error")
}

type MltResultParser interface {
	Parse(*[]byte) (*SolrMltResult, error)
}

type MoreLikeThisParser struct {
}

func (parser *MoreLikeThisParser) Parse(resp_ *[]byte) (*SolrMltResult, error) {
	jsonbuf, err := bytes2json(resp_)
	sr := &SolrMltResult{}
	if err != nil {
		return sr, nil
	}
	var resp = new(SolrResponse)
	resp.Response = jsonbuf
	rHeader, convOk := jsonbuf["responseHeader"].(map[string]interface{})
	if !convOk {
		return nil, fmt.Errorf("Parse - type casting error")
	}

	resp.Status = int(rHeader["status"].(float64))

	sr.Results = new(Collection)
	sr.Match = new(Collection)
	sr.Status = resp.Status

	if responseHeader, ok := resp.Response["responseHeader"].(map[string]interface{}); ok {
		sr.ResponseHeader = responseHeader
	} else {
		return nil, fmt.Errorf("Parse - type casting error")
	}

	if resp.Status == 0 {
		if resp, ok := resp.Response["response"].(map[string]interface{}); ok {
			err = ParseDocResponse(resp, sr.Results)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("Parse - type casting error")
		}

		if match, ok := resp.Response["match"].(map[string]interface{}); ok {
			err = ParseDocResponse(match, sr.Match)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("Parse - type casting error")
		}
	} else {
		if err, ok := resp.Response["error"].(map[string]interface{}); ok {
			sr.Error = err
		} else {
			return nil, fmt.Errorf("Parse - type casting error")
		}
	}
	return sr, nil
}
