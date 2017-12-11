package tools

type esKS struct {
	httpT *httpTool
}

func (ts *esKS) init(httpT *httpTool) {
	ts.httpT = httpT
}

func (ts *esKS) Delete(ksid string) bool {

	returnCode, _, err := ts.httpT.DELETE(ksid)

	return returnCode == 200 && err == nil
}

func (ts *esKS) GetIndex(ksid string) []byte {

	_, response, _ := ts.httpT.GET(ksid)

	return response
}
