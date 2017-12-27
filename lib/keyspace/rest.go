package keyspace

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	storage "github.com/uol/mycenae/lib/persistence"
)

// Create is a rest endpoint to create a keyspace
func (kspace *Keyspace) Create(
	w http.ResponseWriter, r *http.Request, ps httprouter.Params,
) {
	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r,
			map[string]string{
				"path":     "/keyspaces/#keyspace",
				"keyspace": "empty",
			},
		)
		rip.Fail(w, errNotFound("Create"))
		return
	}

	if !storage.ValidateKey(ks) {
		rip.AddStatsMap(r,
			map[string]string{
				"path": "/keyspaces/#keyspace",
			},
		)
		rip.Fail(w, errValidationS(
			"CreateKeyspace",
			`Wrong Format: Field "keyspaceName" is not well formed. NO information will be saved`,
		))
		return
	}

	rip.AddStatsMap(r,
		map[string]string{
			"path":     "/keyspaces/#keyspace",
			"keyspace": ks,
		},
	)

	var ksc Config
	err := rip.FromJSON(r, &ksc)
	if err != nil {
		rip.Fail(w, err)
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
	ksid, err := kspace.storage.CreateKeyspace(ksc)
	if err != nil {
		rip.Fail(w, err)
		return
	}

	out := CreateResponse{
		Ksid: ks,
	}

	rip.SuccessJSON(w, http.StatusCreated, out)
	return
}

// Update is a rest endpoint that takes care of updating the keyspace metadata
// information
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

	gerr = kspace.UpdateKeyspace(ksc, ks)
	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace"})
		rip.Fail(w, gerr)
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace", "keyspace": ks})

	rip.Success(w, http.StatusOK, nil)
	return
}

// GetAll is a rest endpoint that returns all the datacenters
func (kspace *Keyspace) GetAll(
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) {
	keyspaces, err := kspace.ListKeyspaces()
	if err != nil {
		rip.Fail(w, err)
		return
	}
	total := len(keyspaces)

	if total <= 0 {
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

// Check verifies if a keyspace exists
func (kspace *Keyspace) Check(
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) {
	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path":     "/keyspaces/#keyspace",
				"keyspace": "empty",
			},
		)
		rip.Fail(w, errNotFound("Check"))
		return
	}

	_, found, err := kspace.storage.GetKeyspace(ks)
	if err != nil {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path": "/keyspaces/#keyspace",
			},
		)
		rip.Fail(w, err)
		return
	}
	if !found {
		rip.Fail(w, errNotFound(
			"Check",
		))
		return
	}

	rip.AddStatsMap(
		r,
		map[string]string{
			"path":     "/keyspaces/#keyspace",
			"keyspace": ks,
		},
	)
	rip.Success(w, http.StatusOK, nil)
}

// ListDC lists all the datacenters in the cassandra / scylladb cluster
func (kspace *Keyspace) ListDC(
	w http.ResponseWriter,
	r *http.Request,
	ps httprouter.Params,
) {
	datacenters, err := kspace.ListDatacenters()
	if err != nil {
		rip.Fail(w, err)
		return
	}
	if len(datacenters) == 0 {
		rip.Fail(w, errNoContent("ListDatacenters"))
		return
	}
	rip.SuccessJSON(w, http.StatusOK, Response{
		TotalRecords: len(datacenters),
		Payload:      datacenters,
	})
}
