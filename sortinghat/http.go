package sortinghat

import (
	"fmt"
	. "github.com/zettio/weave/logging"
	"io"
	"log"
	"net/http"
)

func httpErrorAndLog(level *log.Logger, w http.ResponseWriter, msg string,
	status int, logmsg string, logargs ...interface{}) {
	http.Error(w, msg, status)
	level.Printf(logmsg, logargs...)
}

func ListenHttp(port int, space Space) {
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "All good with sortinghat")
	})
	http.HandleFunc("/ip/", func(w http.ResponseWriter, r *http.Request) {
		if newAddr := space.Allocate(); newAddr != nil {
			io.WriteString(w, newAddr.String())
		} else {
			httpErrorAndLog(
				Error, w, "Internal error", http.StatusInternalServerError,
				"No free addresses")
		}
	})

	address := fmt.Sprintf(":%d", port)
	if err := http.ListenAndServe(address, nil); err != nil {
		Error.Fatal("Unable to create http listener: ", err)
	}
}
