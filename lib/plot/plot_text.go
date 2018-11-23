package plot

import (
	"regexp"
	"sort"

	"github.com/uol/gobol"

	"strconv"
)

func (plot *Plot) GetTextSeries(
	ttl uint8,
	keys []string,
	start,
	end int64,
	mergeType string,
	keepEmpties bool,
	search *regexp.Regexp,
) (TST, gobol.Error) {

	var keyspace string
	var ok bool
	if keyspace, ok = plot.keyspaceTTLMap[ttl]; !ok {
		return TST{}, errNotFound("invalid ttl found: " + strconv.Itoa(int(ttl)))
	}

	tsMap, gerr := plot.getTextSerie(keyspace, keys, start, end, keepEmpties, search)

	if gerr != nil {
		return TST{}, gerr
	}

	resultTSTs := TST{}
	numNonEmptyTST := 0

	for _, ts := range tsMap {

		if ts.Count > 0 {
			numNonEmptyTST++
			resultTSTs.Data = append(resultTSTs.Data, ts.Data...)
			resultTSTs.Total += ts.Total
			resultTSTs.Count += ts.Count
		}
	}

	if numNonEmptyTST > 1 {
		sort.Sort(resultTSTs.Data)
	}

	return resultTSTs, nil
}

func (plot *Plot) getTextSerie(
	keyspace string,
	keys []string,
	start,
	end int64,
	keepEmpties bool,
	search *regexp.Regexp,
) (map[string]TST, gobol.Error) {

	resultMap, gerr := plot.persist.GetTST(keyspace, keys, start, end, search)

	if gerr != nil {
		return map[string]TST{}, gerr
	}

	transformedMap := map[string]TST{}

	for tsid, points := range resultMap {
		total := len(points)
		ts := TST{
			Total: total,
			Count: total,
			Data:  points,
		}
		transformedMap[tsid] = ts
	}

	return transformedMap, nil
}
