package plot

import (
	"sort"

	"github.com/uol/gobol"

	"strconv"

	"github.com/uol/mycenae/lib/structs"
)

const (
	milliWeek = 6.048e+8
)

func (plot *Plot) GetTimeSeries(
	ttl uint8,
	keys []string,
	start,
	end int64,
	opers structs.DataOperations,
	ms,
	keepEmpties bool,
) (TS, gobol.Error) {

	var keyspace string
	var ok bool
	if keyspace, ok = plot.keyspaceTTLMap[ttl]; !ok {
		return TS{}, errNotFound("invalid ttl found: " + strconv.Itoa(int(ttl)))
	}

	tsMap, gerr := plot.getTimeSerie(keyspace, keys, start, end, ms, keepEmpties, opers)

	if gerr != nil {
		return TS{}, gerr
	}

	resultTSs := TS{}
	numNonEmptyTS := 0

	for _, ts := range tsMap {

		if ts.Count > 0 {
			numNonEmptyTS++
			resultTSs.Data = append(resultTSs.Data, ts.Data...)
			resultTSs.Total += ts.Total
		}
	}

	exec := false
	for _, oper := range opers.Order {
		switch oper {
		case "downsample":
			if resultTSs.Total > 0 && opers.Downsample.Enabled && exec {
				resultTSs.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, resultTSs.Data)
			}
		case "aggregation":
			exec = true
			if numNonEmptyTS > 1 {
				sort.Sort(resultTSs.Data)
				resultTSs.Data = merge(opers.Merge, keepEmpties, resultTSs.Data)
			}
		case "rate":
			if opers.Rate.Enabled && exec {
				resultTSs.Data = rate(opers.Rate.Options, resultTSs.Data)
			}
		case "filterValue":
			if opers.FilterValue.Enabled && exec {
				resultTSs.Data = filterValues(opers.FilterValue, resultTSs.Data)
			}
		}
	}

	if opers.Downsample.PointLimit && len(resultTSs.Data) > opers.Downsample.TotalPoints {
		resultTSs.Data = basic(opers.Downsample.TotalPoints, resultTSs.Data)
	}

	resultTSs.Count = len(resultTSs.Data)

	return resultTSs, nil
}

func (plot *Plot) getTimeSerie(
	keyspace string,
	keys []string,
	start,
	end int64,
	ms,
	keepEmpties bool,
	opers structs.DataOperations,
) (map[string]TS, gobol.Error) {

	resultMap, gerr := plot.persist.GetTS(keyspace, keys, start, end, ms)

	if gerr != nil {
		return map[string]TS{}, gerr
	}

	transformedMap := map[string]TS{}

	for tsid, points := range resultMap {
		ts := TS{
			Total: len(points),
			Data:  points,
		}
		for _, oper := range opers.Order {
			exit := false
			switch oper {
			case "downsample":
				if ts.Total > 0 && opers.Downsample.Enabled {
					ts.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, ts.Data)
				}
			case "aggregation":
				exit = true
				break
			case "rate":
				if opers.Rate.Enabled {
					ts.Data = rate(opers.Rate.Options, ts.Data)
				}
			case "filterValue":
				if opers.FilterValue.Enabled {
					ts.Data = filterValues(opers.FilterValue, ts.Data)
				}
			}

			if exit {
				break
			}
		}

		ts.Count = len(ts.Data)
		transformedMap[tsid] = ts
	}

	return transformedMap, nil
}
