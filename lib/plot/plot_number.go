package plot

import (
	"fmt"
	"sort"
	"time"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/structs"
)

const (
	milliWeek = 6.048e+8
)

func (plot *Plot) GetTimeSeries(
	keyspace string,
	keys []string,
	start,
	end int64,
	opers structs.DataOperations,
	ms,
	keepEmpties bool,
) (serie TS, gerr gobol.Error) {

	w := start

	index := 0

	buckets := []string{}

	for {
		t := time.Unix(0, w*1e+6)

		year, week := t.ISOWeek()

		buckets = append(buckets, fmt.Sprintf("%v%v", year, week))

		if w > end {
			break
		}

		w += milliWeek

		index++
	}

	tsChan := make(chan TS, len(keys))

	for _, key := range keys {
		plot.concTimeseries <- struct{}{}
		go plot.getTimeSerie(
			keyspace,
			key,
			buckets,
			start,
			end,
			ms,
			keepEmpties,
			opers,
			tsChan,
		)
	}

	j := 0

	for range keys {

		t := <-tsChan
		if t.gerr != nil {
			gerr = t.gerr
		}
		if t.Count > 0 {
			j++
		}
		serie.Data = append(serie.Data, t.Data...)

		serie.Total += t.Total
	}

	if gerr != nil {
		return TS{}, gerr
	}

	exec := false
	for _, oper := range opers.Order {
		switch oper {
		case "downsample":
			if serie.Total > 0 && opers.Downsample.Enabled && exec {
				serie.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, serie.Data)
			}
		case "aggregation":
			exec = true
			if j > 1 {
				sort.Sort(serie.Data)
				serie.Data = merge(opers.Merge, keepEmpties, serie.Data)
			}
		case "rate":
			if opers.Rate.Enabled && exec {
				serie.Data = rate(opers.Rate.Options, serie.Data)
			}
		case "filterValue":
			if opers.FilterValue.Enabled && exec {
				serie.Data = filterValues(opers.FilterValue, serie.Data)
			}
		}
	}

	if opers.Downsample.PointLimit && len(serie.Data) > opers.Downsample.TotalPoints {
		serie.Data = basic(opers.Downsample.TotalPoints, serie.Data)
	}
	serie.Count = len(serie.Data)

	return serie, nil
}

func (plot *Plot) getTimeSerie(
	keyspace,
	key string,
	buckets []string,
	start,
	end int64,
	ms,
	keepEmpties bool,
	opers structs.DataOperations,
	tsChan chan TS,
) {

	serie := TS{}

	chanSize := len(buckets)
	if chanSize == 0 {
		chanSize = 1
	}

	bucketChan := make(chan TS, chanSize)

	if len(buckets) > 0 {
		for i, bucket := range buckets {
			buckID := fmt.Sprintf("%v%v", bucket, key)
			plot.concReads <- struct{}{}
			go plot.getTimeSerieBucket(i, keyspace, buckID, start, end, ms, bucketChan)
		}
	} else {
		plot.concReads <- struct{}{}
		go plot.getTimeSerieBucket(0, keyspace, key, start, end, ms, bucketChan)
	}

	bucketList := make([]TS, chanSize)

	for i := 0; i < chanSize; i++ {
		buck := <-bucketChan
		bucketList[buck.index] = buck
	}

	for _, bl := range bucketList {
		if bl.gerr != nil {
			serie.gerr = bl.gerr
			tsChan <- serie
			<-plot.concTimeseries
			return
		}

		serie.Data = append(serie.Data, bl.Data...)

		serie.Total += bl.Total

	}

	for _, oper := range opers.Order {
		switch oper {
		case "downsample":
			if serie.Total > 0 && opers.Downsample.Enabled {
				serie.Data = downsample(opers.Downsample.Options, keepEmpties, start, end, serie.Data)
			}
		case "aggregation":
			serie.Count = len(serie.Data)
			tsChan <- serie
			<-plot.concTimeseries
			return
		case "rate":
			if opers.Rate.Enabled {
				serie.Data = rate(opers.Rate.Options, serie.Data)
			}
		case "filterValue":
			if opers.FilterValue.Enabled {
				serie.Data = filterValues(opers.FilterValue, serie.Data)

			}
		}
	}

	serie.Count = len(serie.Data)

	tsChan <- serie
	<-plot.concTimeseries
}

func (plot *Plot) getTimeSerieBucket(
	index int,
	keyspace,
	key string,
	start,
	end int64,
	ms bool,
	bucketChan chan TS,
) {

	resultSet, count, gerr := plot.persist.GetTS(keyspace, key, start, end, ms)

	bucketChan <- TS{
		index: index,
		Total: count,
		Data:  resultSet,
		gerr:  gerr,
	}
	<-plot.concReads
}
