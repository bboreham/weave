package sortinghat

import (
	"fmt"
	wt "github.com/zettio/weave/testing"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func genForm(method string, url string, data url.Values) (resp *http.Response, err error) {
	req, err := http.NewRequest(method, url, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return http.DefaultClient.Do(req)
}

func TestHttp(t *testing.T) {
	var (
		containerID = "deadbeef"
		testAddr1   = "10.0.3.4"
	)

	space := NewSpace(net.ParseIP(testAddr1), 3)
	port := rand.Intn(10000) + 32768
	fmt.Println("Http test on port", port)
	go ListenHttp(port, space)

	time.Sleep(100 * time.Millisecond) // Allow for http server to get going

	// Ask the http server for a new address
	addrUrl := fmt.Sprintf("http://localhost:%d/ip/%s", port, containerID)
	resp, err := http.Get(addrUrl)
	wt.AssertNoErr(t, err)
	wt.AssertStatus(t, resp.StatusCode, http.StatusOK, "http response")
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if string(body) != testAddr1 {
		t.Fatalf("Expected address %s but got %s", testAddr1, string(body))
	}

	// Would like to shut down the http server at the end of this test
	// but it's complicated.
	// See https://groups.google.com/forum/#!topic/golang-nuts/vLHWa5sHnCE
}
