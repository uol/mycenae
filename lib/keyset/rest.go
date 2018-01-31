package keyset

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func (ks *KeySet) CreateKeySet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keySetParam := ps.ByName("keyset")

	if keySetParam == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyset/#keyset", "keyset": "empty"})
		rip.Fail(w, errBadRequest("CreateKeySet", "parameter 'keyset' cannot be empty"))
		return
	}

	if !ks.keySetRegexp.MatchString(keySetParam) {
		rip.AddStatsMap(r, map[string]string{"path": "/keyset/#keyset"})
		rip.Fail(w, errBadRequest("CreateKeySet", "parameter 'keyset' has an invalid format"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyset/#keyset", "keyset": keySetParam})

	exists, gerr := ks.KeySetExists(keySetParam)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if exists {
		rip.Success(w, http.StatusConflict, nil)
	} else {
		gerr := ks.CreateIndex(keySetParam)
		if gerr != nil {
			rip.Fail(w, gerr)
			return
		}
		rip.Success(w, http.StatusCreated, nil)
	}

	return
}

func (ks *KeySet) GetKeySets(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keySetMap, gerr := ks.getKeySetMap()

	if gerr != nil {
		rip.AddStatsMap(r, map[string]string{"path": "/keysets"})
		rip.Fail(w, errInternalServerError("GetKeySets", gerr))
		return
	}

	if keySetMap == nil {
		rip.SuccessJSON(w, http.StatusNoContent, nil)
	} else {
		keySets := []string{}
		for k, _ := range keySetMap {
			keySets = append(keySets, k)
		}
		rip.SuccessJSON(w, http.StatusOK, keySets)
	}

	return
}