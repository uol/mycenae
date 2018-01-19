package keyspace

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func (kspace *Keyspace) Create(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Create"))
		return
	}

	if !validKey.MatchString(ks) {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace"})
		rip.Fail(w, errValidationS(
			"CreateKeyspace",
			`Wrong Format: Field "keyspaceName" is not well formed. NO information will be saved`,
		))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": ks})

	ksc := Config{}

	gerr := rip.FromJSON(r, &ksc)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if ksc.TTL <= 0 {
		rip.Fail(w, errValidationS("CreateKeyspace", "'ttl' is required"))
	} else if ksc.Contact == "" {
		rip.Fail(w, errValidationS("CreateKeyspace", "'contact' is required"))
	} else if ksc.Datacenter == "" {
		rip.Fail(w, errValidationS("CreateKeyspace", "'datacenter' is required"))
	} else if ksc.ReplicationFactor <= 0 {
		rip.Fail(w, errValidationS("CreateKeyspace", "'replicationFactor' is required"))
	}

	ksc.Name = ks

	gerr = kspace.CreateKeyspace(ksc)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	out := CreateResponse{
		Ksid: ks,
	}

	rip.SuccessJSON(w, http.StatusCreated, out)
	return
}

func (kspace *Keyspace) Update(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Update"))
		return
	}

	ksc := ConfigUpdate{}

	gerr := rip.FromJSON(r, &ksc)
	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace"})
		rip.Fail(w, gerr)
		return
	}

	gerr = kspace.updateKeyspace(ksc, ks)
	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace"})
		rip.Fail(w, gerr)
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": ks})

	rip.Success(w, http.StatusOK, nil)
	return
}

func (kspace *Keyspace) GetAll(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspaces, total, gerr := kspace.listAllKeyspaces()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(keyspaces) == 0 {
		gerr := errNoContent("ListAllKeyspaces")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      keyspaces,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

func (kspace *Keyspace) Check(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Check"))
		return
	}

	gerr := kspace.checkKeyspace(ks)
	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace"})
		rip.Fail(w, gerr)
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": ks})

	rip.Success(w, http.StatusOK, nil)
	return
}

func (kspace *Keyspace) ListDC(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	datacenters, gerr := kspace.listDatacenters()
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(datacenters) == 0 {
		gerr := errNoContent("ListDatacenters")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: len(datacenters),
		Payload:      datacenters,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}