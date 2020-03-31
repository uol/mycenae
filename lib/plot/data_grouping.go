package plot

import (
	"math"
	"time"

	"github.com/uol/mycenae/lib/structs"
)

const (
	msSec  = 1000
	msMin  = 60000
	msHour = 3.6e+6
	msDay  = 8.64e+7
	msWeek = 6.048e+8
)

func basic(totalPoints int, serie []Pnt) (groupSerie []Pnt) {

	total := len(serie)

	group := float64(total) / float64(totalPoints)

	group = round(group, .5, 0)

	var counter float64

	var avgCounter float64

	var nilCounter float64

	var groupDate int64

	var groupValue float64

	for i, point := range serie {

		groupDate += point.Date

		if point.Empty {

			nilCounter++

		} else {

			groupValue += point.Value

			avgCounter++

		}

		counter++

		if counter == group || i == total-1 {

			groupDate = groupDate / int64(counter)

			groupValue = groupValue / avgCounter

			var groupPoint Pnt

			groupPoint.Date = groupDate

			groupPoint.Value = groupValue

			if nilCounter == counter {
				groupPoint.Empty = true
			}

			groupSerie = append(groupSerie, groupPoint)

			counter = 0

			avgCounter = 0

			nilCounter = 0

			groupDate = 0

			groupValue = 0

		}

	}

	return

}

func rate(options structs.TSDBrateOptions, serie Pnts) Pnts {

	if len(serie) == 1 {
		return serie
	}

	rateSerie := Pnts{}

	for i := 1; i < len(serie); i++ {

		if serie[i].Empty || serie[i-1].Empty {
			p := Pnt{
				Date:  serie[i].Date,
				Empty: true,
			}
			rateSerie = append(rateSerie, p)
			continue
		}

		var value float64

		if options.Counter && serie[i].Value < serie[i-1].Value {
			value = (float64(*options.CounterMax) + serie[i].Value - serie[i-1].Value) / float64((serie[i].Date/int64(1000))-(serie[i-1].Date/int64(1000)))
			if options.ResetValue != 0 && float64(options.ResetValue) <= value {
				value = 0
			}
		} else {
			value = (serie[i].Value - serie[i-1].Value) / float64((serie[i].Date/int64(1000))-(serie[i-1].Date/int64(1000)))
		}

		p := Pnt{
			Value: value,
			Date:  serie[i].Date,
			Empty: false,
		}

		rateSerie = append(rateSerie, p)
	}

	return rateSerie
}

func downsample(options structs.DSoptions, keepEmpties bool, start, end int64, serie Pnts) Pnts {

	startDate := time.Unix(0, start*1e+6)

	switch options.Unit {
	case "sec":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			startDate.Minute(),
			startDate.Second(),
			0,
			time.Local,
		)
		start = base.Unix() * 1e+3
	case "min":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			startDate.Minute(),
			0,
			0,
			time.Local,
		)
		start = base.Unix() * 1e+3
	case "hour":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			0,
			0,
			0,
			time.Local,
		)
		start = base.Unix() * 1e+3
	case "day":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		start = base.Unix() * 1e+3
	case "week":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		for base.Weekday() != time.Monday {
			base = base.AddDate(0, 0, -1)
		}
		start = base.Unix() * 1e+3
	case "month":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		for base.Month() == startDate.Month() {
			base = base.AddDate(0, 0, -1)
		}
		base = base.AddDate(0, 0, 1)
		start = base.Unix() * 1e+3
	case "year":
		base := time.Date(startDate.Year(), time.January, 1, 0, 0, 0, 0, time.Local)
		start = base.Unix() * 1e+3
	}

	groupDate := start

	endInterval := getEndInterval(start, options.Unit, options.Value)

	var groupedCount float64

	groupedPoint := Pnt{}

	groupedSerie := Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		//Ajusting for missing points
		for point.Date >= endInterval {
			if keepEmpties {
				groupedPoint.Date = groupDate

				if options.Fill == "zero" {
					groupedPoint.Value = 0
				} else {
					groupedPoint.Empty = true
				}

				groupedSerie = append(groupedSerie, groupedPoint)

				groupedPoint = Pnt{}
			}

			groupDate = endInterval

			endInterval = getEndInterval(endInterval, options.Unit, options.Value)
		}

		groupedCount++

		switch options.Downsample {
		case "avg":
			groupedPoint.Value += point.Value
		case "sum":
			groupedPoint.Value += point.Value
		case "max":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value > groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "min":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value < groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "pnt":
			groupedPoint.Value = groupedCount
		}

		if i+1 == len(serie) || serie[i+1].Date >= endInterval {

			groupedPoint.Date = groupDate

			groupDate = endInterval

			if options.Downsample == "avg" {
				groupedPoint.Value = groupedPoint.Value / groupedCount
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			groupedCount = 0

			groupedPoint = Pnt{}

			if i+1 != len(serie) {
				endInterval = getEndInterval(endInterval, options.Unit, options.Value)
			}
		}

	}

	if keepEmpties {
		for i := endInterval; i < end; i = endInterval {

			groupedPoint := Pnt{
				Date: endInterval,
			}

			if options.Fill == "zero" {
				groupedPoint.Value = 0
			} else {
				groupedPoint.Empty = true
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			endInterval = getEndInterval(i, options.Unit, options.Value)
		}
	}

	return groupedSerie
}

func getEndInterval(start int64, unit string, value int) int64 {

	var end int64

	switch unit {
	case "ms":
		end = start + int64(value)
	case "sec":
		end = start + msSec*int64(value)
	case "min":
		end = start + msMin*int64(value)
	case "hour":
		end = start + msHour*int64(value)
	case "day":
		end = start + msDay*int64(value)
	case "week":
		end = start + msWeek*int64(value)
	case "month":
		startDate := time.Unix(0, start*1e+6)

		base := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.Local)

		base = base.AddDate(0, value, 0)

		end = base.Unix() * 1e+3
	case "year":
		startDate := time.Unix(0, start*1e+6)

		base := time.Date(startDate.Year(), time.January, 1, 0, 0, 0, 0, time.Local)

		base = base.AddDate(value, 0, 0)

		end = base.Unix() * 1e+3
	default:
		return end
	}

	return end
}

func fuseNumber(first, second Pnts) Pnts {

	sizeFirst := len(first)
	sizeScond := len(second)

	fused := make(Pnts, sizeFirst+sizeScond)
	var i, j, k int

	for i < sizeFirst && j < sizeScond {

		if first[i].Date <= second[j].Date {
			fused[k] = first[i]
			i++
		} else {
			fused[k] = second[j]
			j++
		}

		k++
	}
	if i < sizeFirst {
		for p := i; p < sizeFirst; p++ {
			fused[k] = first[p]
			k++
		}
	} else {
		for p := j; p < sizeScond; p++ {
			fused[k] = second[p]
			k++
		}
	}

	return fused
}

func merge(mergeType string, keepEmpties bool, serie Pnts) Pnts {

	mergedSerie := Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		var mergedPoint Pnt

		if i < len(serie)-1 {

			j := i + 1

			nextPoint := serie[j]

			var mergedCount, nullCount float64

			mergedPoint = point

			mergedCount++

			if point.Empty {
				nullCount++
			}

			for point.Date == nextPoint.Date {

				i++

				mergedCount++

				if !nextPoint.Empty {

					mergedPoint.Empty = false

					switch mergeType {
					case "avg":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "sum":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "max":
						if nextPoint.Value > mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "min":
						if nextPoint.Value < mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "pnt":
						mergedPoint.Value = mergedCount
					}
				} else {
					nullCount++
				}

				if j == len(serie)-1 {
					break
				}

				j++

				nextPoint = serie[j]
			}

			if mergedCount-nullCount > 1 {

				if mergeType == "avg" {
					mergedPoint.Value = mergedPoint.Value / mergedCount
				}

			} else if nullCount == mergedCount && keepEmpties {
				mergedPoint.Empty = true
			}

		} else {
			mergedPoint = point
		}

		mergedSerie = append(mergedSerie, mergedPoint)

	}

	return mergedSerie
}

func filterValues(oper structs.FilterValueOperation, serie Pnts) Pnts {

	filteredSerie := Pnts{}

	switch oper.BoolOper {
	case "<":
		for _, pnt := range serie {
			if pnt.Value < oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">":
		for _, pnt := range serie {
			if pnt.Value > oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "==":
		for _, pnt := range serie {
			if pnt.Value == oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">=":
		for _, pnt := range serie {
			if pnt.Value >= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "<=":
		for _, pnt := range serie {
			if pnt.Value <= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "!=":
		for _, pnt := range serie {
			if pnt.Value != oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	}

	return filteredSerie
}

func round(val float64, roundOn float64, places int) (newVal float64) {

	var round float64

	pow := math.Pow(10, float64(places))

	digit := pow * val

	_, div := math.Modf(digit)

	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}

	newVal = round / pow

	return

}

func msToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

func timeToMs(date time.Time) (ms int64) {
	return date.UnixNano() / int64(time.Millisecond)
}
