package main

import (
	"hash/crc32"
	"sort"
	"fmt"
)

func main(){
	tags := map[string]string{"ksid": "ts_591527516", "ttl": "90", "host": "a1-kolombo1"}
	id := GetTextHashFromMetricAndTags("os.cpu", tags)
	fmt.Println(id)
}

func GetTextHashFromMetricAndTags(metric string, tags map[string]string) string {
	h := crc32.NewIEEE()
	h.Write([]byte(metric))
	mk := []string{}

	for k := range tags {
		if k != "tuuid" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {
		h.Write([]byte(k))
		h.Write([]byte(tags[k]))
	}

	return fmt.Sprint("T", h.Sum32())
}