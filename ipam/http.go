package ipam

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/address"
)

func badRequest(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusBadRequest)
	common.Warning.Println("[allocator]:", err.Error())
}

// HandleHTTP wires up ipams HTTP endpoints to the provided mux.
func (alloc *Allocator) HandleHTTP(router *mux.Router) {
	router.Methods("PUT").Path("/ip/{id}/{ip}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		closedChan := w.(http.CloseNotifier).CloseNotify()
		vars := mux.Vars(r)
		ident := vars["id"]
		ipStr := vars["ip"]
		if ip, err := address.ParseIP(ipStr); err != nil {
			badRequest(w, err)
			return
		} else if err := alloc.Claim(ident, ip, closedChan); err != nil {
			badRequest(w, fmt.Errorf("Unable to claim: %s", err))
			return
		}

		w.WriteHeader(204)
	})

	router.Methods("POST").Path("/ip/{id}/{ip}/{prefixlen}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		closedChan := w.(http.CloseNotifier).CloseNotify()
		vars := mux.Vars(r)
		ident := vars["id"]
		cidr := vars["ip"] + "/" + vars["prefixlen"]
		if addr, cidr, err := address.ParseCIDR(cidr); err != nil {
			badRequest(w, err)
			return
		} else if addr == cidr.Start {
			newAddr, err := alloc.AllocateInSubnet(ident, cidr, closedChan)
			if err != nil {
				badRequest(w, fmt.Errorf("Unable to allocate: %s", err))
				return
			}

			fmt.Fprintf(w, "%s/%d", newAddr.String(), cidr.PrefixLen)
		} else {
			fmt.Fprintf(w, "%s/%d", addr.String(), cidr.PrefixLen)
		}
	})

	router.Methods("POST").Path("/ip/{id}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if alloc.defaultSubnet.Blank() {
			// error fixme
		}
		closedChan := w.(http.CloseNotifier).CloseNotify()
		ident := mux.Vars(r)["id"]
		newAddr, err := alloc.AllocateInSubnet(ident, alloc.defaultSubnet, closedChan)
		if err != nil {
			badRequest(w, err)
			return
		}

		fmt.Fprintf(w, "%s/%d", newAddr.String(), alloc.defaultSubnet.PrefixLen)
	})

	router.Methods("DELETE").Path("/ip/{id}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ident := mux.Vars(r)["id"]
		if err := alloc.Free(ident); err != nil {
			badRequest(w, err)
			return
		}

		w.WriteHeader(204)
	})

	router.Methods("DELETE").Path("/peer").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alloc.Shutdown()
		w.WriteHeader(204)
	})

	router.Methods("DELETE").Path("/peer/{id}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ident := mux.Vars(r)["id"]
		if err := alloc.AdminTakeoverRanges(ident); err != nil {
			badRequest(w, err)
			return
		}

		w.WriteHeader(204)
	})
}
