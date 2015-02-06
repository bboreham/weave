package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

var (
	listen = flag.String("listen", "localhost:1080", "listen on address")
	dest   = flag.String("dest", "http://localhost:6784", "destination address")
)

func main() {
	flag.Parse()
	target, err := url.Parse(*dest)
	if err != nil {
		log.Fatal(err)
	}

	// Proxy, copying the incoming request exactly apart from the host
	myProxy := &httputil.ReverseProxy{Director: func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
	}}

	// Allow browser requests, no matter what their origin
	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		myProxy.ServeHTTP(w, r)
	})
	log.Fatal(http.ListenAndServe(*listen, proxyHandler))
}
