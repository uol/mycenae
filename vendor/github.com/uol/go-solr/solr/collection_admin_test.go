package solr

import (
	"net/url"
	"testing"
)

func TestCollectionsAdminWrongAction(t *testing.T) {

	ca, _ := NewCollectionsAdmin("http://127.0.0.1:12345/solr")

	params := &url.Values{}
	params.Add("core", "core0")
	_, err := ca.Action("BLABLA", params)
	if err == nil {
		t.Errorf("Should be an error")
	}
	expected := "Action 'BLABLA' not supported"
	if err.Error() != expected {
		t.Errorf("expected error message '%s' but got '%s'", expected, err.Error())
	}
}

func TestCollectionsAdminInvalidUrl(t *testing.T) {
	_, err := NewCollectionsAdmin("sdff")
	if err == nil {
		t.Errorf("Expected an error")
		return
	}
	expected := "parse sdff: invalid URI for request"
	if err.Error() != expected {
		t.Errorf("expected '%s' but got '%s'", expected, err.Error())
	}
}
