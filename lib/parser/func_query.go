package parser

import (
	"fmt"
	"sort"
	"strings"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/constants"
	"github.com/uol/mycenae/lib/structs"
)

func parseQuery(exp string, tsdb *structs.TSDBquery) (e string, err gobol.Error) {

	params := parseParams(string(exp[5:]))

	if len(params) != 3 {
		return constants.StringsEmpty, errParams(
			"parseQuery",
			"query needs 3 parameters: metric, map or null and a time interval",
			fmt.Errorf("query expects 3 parameters but found %d: %v", len(params), params),
		)
	}

	tags := map[string][]string{}

	if params[1] != "null" {
		tags, err = parseMap(params[1])
		if err != nil {
			return constants.StringsEmpty, err
		}
	}

	tsdb.Metric = params[0]

	tsdb.Tags = map[string]string{}

	for k, vs := range tags {
		for _, v := range vs {

			var ft, cv string

			if strings.HasPrefix(v, "regexp(") && strings.HasSuffix(v, ")") {
				ft = "regexp"
				cv = v[7 : len(v)-1]
			} else if strings.HasPrefix(v, "wildcard(") && strings.HasSuffix(v, ")") {
				ft = "wildcard"
				cv = v[9 : len(v)-1]
			} else if strings.HasPrefix(v, "or(") && strings.HasSuffix(v, ")") {
				ft = "literal_or"
				cv = v[3 : len(v)-1]
			} else if strings.HasPrefix(v, "notor(") && strings.HasSuffix(v, ")") {
				ft = "not_literal_or"
				cv = v[6 : len(v)-1]
			} else {
				ft = "wildcard"
				cv = v
			}

			filter := structs.TSDBfilter{
				Ftype:   ft,
				Tagk:    k,
				Filter:  cv,
				GroupBy: false,
			}
			tsdb.Filters = append(tsdb.Filters, filter)
		}
	}

	for _, oper := range tsdb.Order {
		if oper == "query" {
			return constants.StringsEmpty, errDoubleFunc("parseQuery", "query")
		}
	}

	tsdb.Order = append([]string{"query"}, tsdb.Order...)

	return params[2], nil
}

func writeQuery(metric, relative string, filters []structs.TSDBfilter) string {

	exp := fmt.Sprintf("query(%s,", metric)

	if len(filters) == 0 {
		exp = fmt.Sprintf("%snull,", exp)
	} else {

		orderedTags := []string{}

		joinFilters := map[string][]string{}

		for _, filter := range filters {
			if !filter.GroupBy {
				if _, ok := joinFilters[filter.Tagk]; !ok {
					switch filter.Ftype {
					case "wildcard":
						joinFilters[filter.Tagk] = []string{
							filter.Filter,
						}
					case "regexp":
						joinFilters[filter.Tagk] = []string{
							fmt.Sprintf("%s(%s)", filter.Ftype, filter.Filter),
						}
					case "literal_or":
						joinFilters[filter.Tagk] = []string{
							fmt.Sprintf("or(%s)", filter.Filter),
						}
					case "not_literal_or":
						joinFilters[filter.Tagk] = []string{
							fmt.Sprintf("notor(%s)", filter.Filter),
						}
					}
					orderedTags = append(orderedTags, filter.Tagk)
				} else {
					switch filter.Ftype {
					case "wildcard":
						joinFilters[filter.Tagk] = append(
							joinFilters[filter.Tagk],
							filter.Filter,
						)
					case "regexp":
						joinFilters[filter.Tagk] = append(
							joinFilters[filter.Tagk],
							fmt.Sprintf("%s(%s)", filter.Ftype, filter.Filter),
						)
					case "literal_or":
						joinFilters[filter.Tagk] = append(
							joinFilters[filter.Tagk],
							fmt.Sprintf("or(%s)", filter.Ftype),
						)
						joinFilters[filter.Tagk] = []string{
							fmt.Sprintf("or(%s)", filter.Filter),
						}
					case "not_literal_or":
						joinFilters[filter.Tagk] = append(
							joinFilters[filter.Tagk],
							fmt.Sprintf("notor(%s)", filter.Ftype),
						)
					}
				}
			}
		}

		if len(orderedTags) == 0 {
			exp = fmt.Sprintf("%snull,", exp)
		} else {

			expMap := fmt.Sprintf("%s{", exp)

			sort.Strings(orderedTags)

			for _, tk := range orderedTags {

				sort.Strings(joinFilters[tk])

				for _, fv := range joinFilters[tk] {
					expMap = fmt.Sprintf("%s%s=%s,", expMap, tk, fv)
				}
			}

			expMap = expMap[:len(expMap)-1]

			exp = fmt.Sprintf("%s},", expMap)
		}

	}

	exp = fmt.Sprintf("%s%s)", exp, relative)

	return exp
}
