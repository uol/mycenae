package plot

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/structs"
)

func (plot *Plot) GetTextSeries(
	keyspace string,
	keys []string,
	start,
	end int64,
	mergeType string,
	keepEmpties bool,
	search *regexp.Regexp,
	downsample structs.Downsample,
) (serie TST, gerr gobol.Error) {

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

	tsChan := make(chan TST, len(keys))

	for _, key := range keys {
		plot.concTimeseries <- struct{}{}
		go plot.getTextSerie(keyspace, key, buckets, start, end, keepEmpties, search, downsample, tsChan)
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

		serie.Total = t.Total
	}

	if gerr != nil {
		return TST{}, gerr
	}

	if j > 1 {
		sort.Sort(serie.Data)
		serie.Total = len(serie.Data)
	}

	serie.Count = len(serie.Data)

	return serie, nil
}

func (plot *Plot) getTextSerie(
	keyspace,
	key string,
	buckets []string,
	start,
	end int64,
	keepEmpties bool,
	search *regexp.Regexp,
	downsample structs.Downsample,
	tsChan chan TST,
) {

	serie := TST{}

	chanSize := len(buckets)
	bucketChan := make(chan TST, chanSize)

	for i, bucket := range buckets {
		buckID := fmt.Sprintf("%v%v", bucket, key)
		plot.concReads <- struct{}{}
		go plot.getTextSerieBucket(i, keyspace, buckID, start, end, search, downsample, bucketChan)
	}

	bucketList := make([]TST, chanSize)

	for i := 0; i < chanSize; i++ {
		buck := <-bucketChan
		bucketList[buck.index] = buck
	}

	for _, bl := range bucketList {
		if bl.gerr != nil {
			serie.gerr = bl.gerr
			tsChan <- serie
			return
		}

		serie.Data = append(serie.Data, bl.Data...)

		serie.Total += bl.Total
	}

	serie.Count = len(serie.Data)

	tsChan <- serie
	<-plot.concTimeseries
}

func (plot *Plot) getTextSerieBucket(
	index int,
	keyspace,
	key string,
	start,
	end int64,
	search *regexp.Regexp,
	downsample structs.Downsample,
	tsChan chan TST,
) {

	resultSet, count, gerr := plot.persist.GetTST(keyspace, key, start, end, search)

	tsChan <- TST{
		index: index,
		Total: count,
		Data:  resultSet,
		gerr:  gerr,
	}
	<-plot.concReads
}
