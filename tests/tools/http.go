package tools

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type httpTool struct {
	hostname string
	port     string
	timeout  time.Duration
	client   *http.Client
}

type User struct {
	Name     string
	PassWord string
}

type Headers struct {
	User   *User
	Header map[string]string
	Cookie *bytes.Buffer
}

func (hT *httpTool) Init(hostname string, port string, timeout time.Duration) {
	hT.hostname = hostname
	hT.port = port
	hT.timeout = timeout

	hT.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: hT.timeout,
	}
}

func (hT *httpTool) POSTstring(url string, payload string) (statusCode int, respData []byte) {
	statusCode, respData, _ = hT.POST(url, []byte(payload))
	return
}

func (hT *httpTool) POSTjson(url string, postData interface{}, respData interface{}) (statusCode int) {
	payload, err := json.Marshal(postData)
	if err != nil {
		log.Println("HTTPclient:", "postJSON:", "Marshal:", err)
		return
	}
	headers := map[string]string{"Content-Type": "application/json"}
	statusCode, respBytes, _ := hT.request("POST", url, payload, false, headers)

	if len(respBytes) > 0 {
		err = json.Unmarshal(respBytes, respData)
		if err != nil {
			log.Println("HTTPclient:", "POSTjson:", "Unmarshal:", err)
		}
	}

	return statusCode
}

func (hT *httpTool) GETjson(url string, respData interface{}) (statusCode int) {
	statusCode, resp, _ := hT.GET(url)
	err := json.Unmarshal(resp, respData)
	if err != nil {
		log.Println("HTTPclient:", "GETjson:", "Unmarshal:", err)
	}
	return
}

func (hT *httpTool) GET(url string) (statusCode int, respData []byte, err error) {
	var payload []byte
	return hT.request("GET", url, payload, false, nil)
}

func (hT *httpTool) CustomHeaderGET(url string, headers map[string]string) (statusCode int, respData []byte, err error) {
	var payload []byte
	return hT.request("GET", url, payload, false, headers)
}

func (hT *httpTool) POST(url string, payload []byte) (statusCode int, respData []byte, err error) {
	statusCode, respData, err = hT.request("POST", url, payload, false, nil)
	return
}

func (hT *httpTool) CustomHeaderPOST(url string, payload []byte, headers map[string]string) (statusCode int, respData []byte, err error) {
	statusCode, respData, err = hT.request("POST", url, payload, false, headers)
	return
}

func (hT *httpTool) POSTgziped(url string, payload []byte) (statusCode int, respData []byte, err error) {
	statusCode, respData, err = hT.request("POST", url, payload, true, nil)
	return
}

func (hT *httpTool) PUT(url string, payload []byte) (statusCode int, respData []byte, err error) {

	statusCode, respData, err = hT.request("PUT", url, payload, false, nil)

	return

}

func (hT *httpTool) DELETE(url string) (statusCode int, respData []byte, err error) {

	var payload []byte

	statusCode, respData, err = hT.request("DELETE", url, payload, false, nil)

	return

}

func (hT *httpTool) request(method string, url string, payload []byte, gzipit bool, headers map[string]string) (statusCode int, respData []byte, err error) {
	fullPath := ""

	if hT.port == "" {
		fullPath = fmt.Sprintf("%v/%v", hT.hostname, url)
	} else {
		fullPath = fmt.Sprintf("%v:%v/%v", hT.hostname, hT.port, url)
	}

	var req *http.Request
	if len(payload) == 0 {
		req, err = http.NewRequest(method, fullPath, nil)
	} else {

		if gzipit {

			var bb bytes.Buffer

			gzw := gzip.NewWriter(&bb)

			if _, err = gzw.Write(payload); err != nil {
				log.Println("HTTPclient:", method, "gzip:", err)
				return
			}

			if err = gzw.Close(); err != nil {
				log.Println("HTTPclient:", method, "gzip:", err)
				return
			}

			req, err = http.NewRequest(method, fullPath, &bb)

			req.Header.Add("Content-Encoding", "gzip")

		} else {
			req, err = http.NewRequest(method, fullPath, bytes.NewBuffer(payload))
		}

		for k, v := range headers {
			req.Header.Add(k, v)
		}
	}

	if err != nil {
		log.Println("HTTPclient:", method, "NewRequest:", err)
		return
	}

	resp, err := hT.client.Do(req)
	if err != nil {
		log.Println("HTTPclient:", method, "Do:", err)
		return
	}

	defer resp.Body.Close()

	statusCode = resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("HTTPclient:", method, "ReadAll:", err)
	}
	return
}
