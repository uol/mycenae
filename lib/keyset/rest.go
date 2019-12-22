package keyset

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/constants"
)

// CreateKeyset - creates a new keyset
func (ks *Manager) CreateKeyset(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keysetParam := ps.ByName(constants.StringsKeyset)

	if keysetParam == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset", constants.StringsKeyset: "empty"})
		rip.Fail(w, errBadRequest("CreateKeyset", "parameter 'keyset' cannot be empty"))
		return
	}

	if !ks.keysetRegexp.MatchString(keysetParam) {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset"})
		rip.Fail(w, errBadRequest("CreateKeyset", "parameter 'keyset' has an invalid format"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset", constants.StringsKeyset: keysetParam})

	exists, gerr := ks.storage.CheckKeyset(keysetParam)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if exists {
		rip.Success(w, http.StatusConflict, nil)
	} else {
		gerr := ks.Create(keysetParam)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		rip.Success(w, http.StatusCreated, nil)
	}

	return
}

// GetKeysets - returns all stored keysets
func (ks *Manager) GetKeysets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keysets, gerr := ks.storage.ListKeysets()

	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets"})
		rip.Fail(w, errInternalServerError("GetKeysets", gerr))
		return
	}

	if keysets == nil || len(keysets) == 0 {
		rip.SuccessJSON(w, http.StatusNoContent, nil)
	} else {
		rip.SuccessJSON(w, http.StatusOK, keysets)
	}

	return
}

// DeleteKeysets - deletes a keyset
func (ks *Manager) DeleteKeysets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keysetParam := ps.ByName(constants.StringsKeyset)

	if keysetParam == constants.StringsEmpty {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset", constants.StringsKeyset: "empty"})
		rip.Fail(w, errBadRequest("DeleteKeysets", "parameter 'keyset' cannot be empty"))
		return
	}

	if !ks.keysetRegexp.MatchString(keysetParam) {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset"})
		rip.Fail(w, errBadRequest("DeleteKeysets", "parameter 'keyset' has an invalid format"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keysets/#keyset", constants.StringsKeyset: keysetParam})

	exists, gerr := ks.storage.CheckKeyset(keysetParam)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if exists {
		gerr := ks.storage.DeleteKeyset(keysetParam)
		if gerr != nil {
			rip.Fail(w, gerr)
		} else {
			rip.Success(w, http.StatusOK, nil)
		}
	} else {
		rip.Fail(w, errNotFound("DeleteKeysets"))
	}

	return
}

// Check if a keyspace exists
func (ks *Manager) Check(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	keyset := ps.ByName(constants.StringsKeyset)
	if keyset == constants.StringsEmpty {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path":                  "/keysets/#keyset",
				constants.StringsKeyset: "empty",
			},
		)
		rip.Fail(w, errNotFound("Check"))
		return
	}

	found, err := ks.storage.CheckKeyset(keyset)
	if err != nil {
		rip.AddStatsMap(
			r,
			map[string]string{
				"path": "/keysets/#keyset",
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
			"path":                  "/keysets/#keyset",
			constants.StringsKeyset: keyset,
		},
	)

	rip.Success(w, http.StatusOK, nil)
}
