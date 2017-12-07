package tools

type esTool struct {
	Timeseries *esTs
	Keyspace   *esKS
}

func (es *esTool) Init(set ElasticsearchSettings) {

	ht := new(httpTool)
	ht.Init(set.Node, set.Port, set.Timeout)

	ts := new(esTs)
	ts.init(ht)

	ks := new(esKS)
	ks.init(ht)

	es.Timeseries = ts
	es.Keyspace = ks

	return
}
