package tools

type solrTool struct {
	Timeseries *esTs
}

func (es *solrTool) Init(s RestAPISettings) {

	ht := new(httpTool)
	ht.Init(s.Node, s.Port, s.Timeout)

	ts := new(esTs)
	ts.init(ht)

	es.Timeseries = ts

	return
}
